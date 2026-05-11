defmodule Journey.Scheduler.Completions do
  @moduledoc false

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  require Logger

  # Records a successful computation. Returns `{:ok, :value_written}` or `{:ok, :no_value_written}`
  # indicating whether the values table was actually updated. The scheduler uses this flag to gate
  # downstream invalidation checks. On deadlock retry exhaustion, returns `{:ok, :no_value_written}`
  # (the abandoned sweeper will re-run the computation later).
  #
  # For `:loop` computations, `result` is a 2-tuple `{disposition, value}` where disposition is one
  # of `:ok`, `:cont_with_fallback`, `:cont_no_fallback`. For non-loop computations, `result` is the
  # unwrapped value (the inner of `{:ok, value}`).
  def record_success(computation, inputs_to_capture, result) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [:success]"

    Logger.debug("#{prefix}: starting.")

    Journey.Scheduler.Helpers.transaction_with_deadlock_retry(
      fn repo ->
        record_success_in_transaction(repo, computation, inputs_to_capture, result)
      end,
      prefix
    )
    |> case do
      {:ok, write_outcome} ->
        {:ok, write_outcome}

      {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
        Logger.warning(
          "#{prefix}: Failed after retries due to deadlock, " <>
            "computation will be retried by abandoned sweeper"
        )

        {:ok, :no_value_written}

      {:error, other} ->
        Logger.error("#{prefix}: Transaction failed with error: #{inspect(other)}")
        raise other
    end
    |> tap(fn _ -> Logger.debug("#{prefix}: done.") end)
  end

  # Records a failed computation. Returns one of:
  #  - :retried — maybe_schedule_a_retry inserted a new attempt; loop iteration N or compute will run again.
  #  - :exhausted — :max_retries reached; terminal failure for this computation.
  #  - :no_state_change — no retry decision made; either the row was no longer :computing (sweeper or
  #    another path took over) or the inner transaction's deadlock-retry helper gave up.
  #
  # The scheduler uses :exhausted to fire f_on_save once with the original {:error, _} shape.
  # :retried and :no_state_change keep the callback silent. The return reflects the committed
  # outcome of the inner transaction body.
  def record_error(computation, error_details, inputs_to_capture) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [:error]"
    Logger.info("#{prefix}: marking as completed. starting.")

    result =
      Journey.Scheduler.Helpers.transaction_with_deadlock_retry(
        fn repo -> record_error_in_transaction(repo, computation, error_details, inputs_to_capture, prefix) end,
        prefix
      )
      |> case do
        {:ok, status} ->
          status

        {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
          Logger.warning(
            "#{prefix}: Failed after retries due to deadlock, " <>
              "computation will be retried by abandoned sweeper"
          )

          :no_state_change

        {:error, other} ->
          Logger.error("#{prefix}: Transaction failed with error: #{inspect(other)}")
          raise other
      end

    Logger.info("#{prefix}: marking as completed. done.")
    result
  end

  defp record_error_in_transaction(repo, computation, error_details, inputs_to_capture, prefix) do
    Logger.info("#{prefix}: marking as completed. transaction starting.")

    current_computation =
      from(c in Computation, where: c.id == ^computation.id)
      |> repo.one!()

    if current_computation.state == :computing do
      record_error_failed_state(repo, computation, error_details, inputs_to_capture, prefix)
    else
      Logger.warning("#{prefix}: computation completed, but it is no longer :computing. (#{current_computation.state})")

      :no_state_change
    end
  end

  defp record_error_failed_state(repo, computation, error_details, inputs_to_capture, prefix) do
    new_revision =
      Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(computation.execution_id, repo)

    now_seconds = System.system_time(:second)

    retry_outcome =
      computation
      |> Ecto.Changeset.change(%{
        error_details: "#{inspect(error_details)}" |> String.trim() |> String.slice(0, 1000),
        completion_time: now_seconds,
        updated_at: now_seconds,
        state: :failed,
        computed_with: inputs_to_capture,
        ex_revision_at_completion: new_revision
      })
      |> repo.update!()
      |> Journey.Scheduler.Retry.maybe_schedule_a_retry(repo)

    Logger.info("#{prefix}: marking as completed. transaction done.")

    case retry_outcome do
      {:retry_scheduled, _} -> :retried
      {:retries_exhausted, _} -> :exhausted
    end
  end

  defp record_success_in_transaction(repo, computation, inputs_to_capture, result) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}]"
    Logger.debug("#{prefix}: starting.")

    execution = computation.execution_id |> Journey.Executions.load(false, true)

    if execution == nil do
      message = "#{prefix}: execution not found: '#{computation.execution_id}'"
      Logger.error(message)
      raise message
    end

    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    if graph == nil do
      message =
        "#{prefix}: graph '#{execution.graph_name}' / '#{execution.graph_version}' is not registered"

      Logger.error(message)
      raise message
    end

    graph_node = Journey.Graph.find_node_by_name(graph, computation.node_name)

    if graph_node == nil do
      message =
        "#{prefix}: graph '#{execution.graph_name}' / '#{execution.graph_version}' does not have node #{computation.node_name}"

      Logger.error(message)
      raise message
    end

    Logger.debug("#{prefix}: marking as completed.")

    current_computation =
      from(c in Computation, where: c.id == ^computation.id)
      |> repo.one!()

    if current_computation.state == :computing do
      new_revision =
        Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(computation.execution_id, repo)

      write_outcome =
        computation.computation_type
        |> case do
          type when type in [:compute, :historian, :archive] ->
            record_result(
              repo,
              graph_node.mutates,
              false,
              computation.node_name,
              computation.execution_id,
              new_revision,
              result,
              type
            )

          :mutate ->
            record_result(
              repo,
              graph_node.mutates,
              graph_node.update_revision_on_change,
              computation.node_name,
              computation.execution_id,
              new_revision,
              result,
              :mutate
            )

          type when type in [:schedule_once, :tick_once, :schedule_recurring, :tick_recurring] ->
            record_result(
              repo,
              graph_node.mutates,
              false,
              computation.node_name,
              computation.execution_id,
              new_revision,
              result,
              type
            )

          :loop ->
            record_loop_result(
              repo,
              current_computation,
              graph_node,
              new_revision,
              result
            )
        end

      # Mark the computation as "completed".
      now_seconds = System.system_time(:second)

      computation
      |> Ecto.Changeset.change(%{
        completion_time: now_seconds,
        updated_at: now_seconds,
        state: :success,
        computed_with: inputs_to_capture,
        ex_revision_at_completion: new_revision
      })
      |> repo.update!()

      Logger.debug("#{prefix}: done. marking as completed.")

      write_outcome
    else
      Logger.warning(
        "#{prefix}: done. computation completed, but it is no longer :computing. (#{current_computation.state})"
      )

      :no_value_written
    end
  end

  # Each record_result/record_loop_result clause returns one of:
  #   :value_written     — the values table was updated with a new revision
  #   :no_value_written  — nothing settable was written (or only an idempotent no-op)
  # The scheduler uses this flag to gate Invalidate.ensure_all_discardable_cleared/2.

  # Compute nodes: only update if value changed (idempotent, like Journey.set/3)
  defp record_result(repo, nil, _update_revision_on_change, node_name, execution_id, new_revision, result, :compute) do
    current_value = get_current_node_value(repo, execution_id, node_name)

    if current_value != result do
      # Value changed - update with new revision to trigger downstream recomputation
      set_value(execution_id, node_name, new_revision, repo, result)
      :value_written
    else
      # Value unchanged - skip update entirely (matching Journey.set/3 behavior)
      Logger.debug("[#{execution_id}] [#{node_name}]: compute node value unchanged, skipping update")
      :no_value_written
    end
  end

  # Schedule nodes: always update (existing behavior)
  defp record_result(repo, nil, _update_revision_on_change, node_name, execution_id, new_revision, result, _node_type) do
    # Record the result in the corresponding value node.
    set_value(
      execution_id,
      node_name,
      new_revision,
      repo,
      result
    )

    :value_written
  end

  # Mutate nodes: update target node based on update_revision_on_change option
  defp record_result(
         repo,
         node_to_mutate,
         update_revision_on_change,
         node_name,
         execution_id,
         new_revision,
         result,
         _node_type
       ) do
    # Update this node to note that the mutation has been computed.
    set_value(
      execution_id,
      node_name,
      new_revision,
      repo,
      "updated #{inspect(node_to_mutate)}"
    )

    # Record the result in the value node being mutated.
    # When update_revision_on_change is true, only update if the value has changed (matching Journey.set/3 behavior).
    # When update_revision_on_change is false, always update the value without updating revision.
    if update_revision_on_change do
      current_value = get_current_node_value(repo, execution_id, node_to_mutate)

      if current_value != result do
        # Value changed - update both value and revision to trigger downstream recomputation
        set_value(
          execution_id,
          node_to_mutate,
          new_revision,
          repo,
          result
        )
      else
        # If value unchanged, skip update entirely (matching Journey.set/3 behavior)
        Logger.debug(
          "[#{execution_id}] [#{node_name}]: mutation target #{inspect(node_to_mutate)} value unchanged, skipping update"
        )
      end
    else
      # update_revision_on_change: false - update value without revision (mutations don't trigger recomputation by default)
      set_value(
        execution_id,
        node_to_mutate,
        nil,
        repo,
        result
      )
    end

    # Mutate node's own value was written (regardless of what happened to the target).
    :value_written
  end

  # ---- :loop node lifecycle --------------------------------------------------
  #
  # A loop is a chain of `computations` rows — one per iteration — threaded by
  # `loop_iteration` (1-indexed). Each row transitions through the standard
  # computation states: :not_set -> :computing -> :completed | :failed | :abandoned.
  #
  # At :completed, the step function's return determines what happens next:
  #
  #   {:ok, v}                  -> values[name] := v; chain ends.
  #   {:cont_with_fallback, v}  -> record carried value in loop_state; insert
  #                                next-iter :not_set row. If current row is at
  #                                max_iterations: values[name] := v instead
  #                                (no next row).
  #   {:cont_no_fallback, v}    -> record carried value in loop_state; insert
  #                                next-iter :not_set row. If at max_iterations:
  #                                f_on_save fires {:error, "max_iterations_reached"}
  #                                and the values table stays unset.
  #   {:error, _}               -> row marked :failed. Retry.maybe_schedule_a_retry
  #                                inserts a new :not_set row with the SAME
  #                                loop_iteration (per-iteration retry budget,
  #                                not global). On exhaustion: f_on_save fires
  #                                the original error; values table stays unset.
  #
  # Heartbeat timeouts use the standard abandonment sweep. On retry exhaustion
  # the terminal callback fires {:error, "timeout"} AFTER transaction commit,
  # to keep at-least-once from sliding into at-least-twice.
  #
  # The values table is written in exactly two cases: terminal :ok and
  # cap-promoted :cont_with_fallback. Every other outcome leaves it unset.
  #
  # Self-reference: on each iteration, values_map.<name> is set to the previous
  # iteration's carried value (read from `loop_state` of the prior :completed
  # row, not from the values table). For iteration 1, values_map.<name> is
  # removed — the values table may hold a settled value from a previous run,
  # but iter 1 must see it as unset.
  #
  # Loop nodes: result is a 2-tuple {disposition, value} where disposition is one of
  # :ok, :cont_with_fallback, :cont_no_fallback. Returns :value_written, :no_value_written,
  # or :loop_cap_failed (terminal cap-failure on :cont_no_fallback at max_iterations).
  defp record_loop_result(repo, current_computation, graph_node, new_revision, {disposition, returned_value}) do
    node_name = current_computation.node_name |> to_atom_if_string()
    execution_id = current_computation.execution_id
    current_iter = current_computation.loop_iteration
    max_iter = graph_node.max_iterations

    cond do
      disposition == :ok ->
        # Terminal success. Write to values table; do not create next iteration.
        # loop_state stays nil on this row (terminal :ok carries no continuation).
        set_value(execution_id, node_name, new_revision, repo, returned_value)
        :value_written

      disposition == :cont_with_fallback and current_iter >= max_iter ->
        # Cap-promotion. Record loop_state for introspection; promote value to terminal.
        mark_loop_state(repo, current_computation, "cont_with_fallback", returned_value)
        set_value(execution_id, node_name, new_revision, repo, returned_value)
        :value_written

      disposition == :cont_no_fallback and current_iter >= max_iter ->
        # Cap-failure. Record loop_state for introspection; do NOT write to values.
        # :loop_cap_failed signals the terminal cap-failure to the f_on_save gate.
        mark_loop_state(repo, current_computation, "cont_no_fallback", returned_value)
        :loop_cap_failed

      disposition in [:cont_with_fallback, :cont_no_fallback] ->
        # Continuation. Record loop_state on this row; insert next iteration's :not_set row.
        mark_loop_state(repo, current_computation, Atom.to_string(disposition), returned_value)
        insert_next_loop_iteration(repo, current_computation, current_iter + 1)
        :no_value_written
    end
  end

  defp mark_loop_state(repo, computation, disposition_string, value) do
    computation
    |> Ecto.Changeset.change(%{
      loop_state: %{"disposition" => disposition_string, "value" => value}
    })
    |> repo.update!()
  end

  defp insert_next_loop_iteration(repo, current_computation, next_iteration) do
    %Journey.Persistence.Schema.Execution.Computation{
      execution_id: current_computation.execution_id,
      node_name: current_computation.node_name,
      computation_type: :loop,
      state: :not_set,
      loop_iteration: next_iteration
    }
    |> repo.insert!()
  end

  defp to_atom_if_string(name) when is_atom(name), do: name
  defp to_atom_if_string(name) when is_binary(name), do: String.to_atom(name)

  defp set_value(execution_id, node_name, new_revision, repo, value) do
    node_name_as_string = node_name |> Atom.to_string()

    now_seconds = System.system_time(:second)

    from(v in Value,
      where: v.execution_id == ^execution_id and v.node_name == ^node_name_as_string
    )
    |> then(fn q ->
      if new_revision == nil do
        q
        |> repo.update_all(
          set: [
            node_value: value,
            updated_at: now_seconds,
            set_time: now_seconds
          ]
        )
      else
        from(v in Value,
          where: v.execution_id == ^execution_id and v.node_name == "last_updated_at"
        )
        |> repo.update_all(
          set: [
            ex_revision: new_revision,
            node_value: now_seconds,
            updated_at: now_seconds,
            set_time: now_seconds
          ]
        )

        q
        |> repo.update_all(
          set: [
            node_value: value,
            set_time: now_seconds,
            updated_at: now_seconds,
            ex_revision: new_revision
          ]
        )
      end
    end)
  end

  defp get_current_node_value(repo, execution_id, node_name) do
    value_node =
      from(v in Value,
        where: v.execution_id == ^execution_id and v.node_name == ^Atom.to_string(node_name)
      )
      |> repo.one()

    case value_node do
      nil -> nil
      node -> Map.get(node, :node_value)
    end
  end
end
