defmodule Journey.Scheduler.Completions do
  @moduledoc false

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  require Logger

  def record_success(computation, inputs_to_capture, result) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [:success]"

    Logger.debug("#{prefix}: starting.")

    {:ok, _} =
      Journey.Scheduler.Helpers.transaction_with_deadlock_retry(
        fn repo ->
          record_success_in_transaction(repo, computation, inputs_to_capture, result)
        end,
        prefix
      )
      |> case do
        {:ok, result} ->
          {:ok, result}

        {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
          Logger.warning(
            "#{prefix}: Failed after retries due to deadlock, " <>
              "computation will be retried by abandoned sweeper"
          )

          {:ok, nil}

        {:error, other} ->
          Logger.error("#{prefix}: Transaction failed with error: #{inspect(other)}")
          raise other
      end
      |> tap(fn _ -> Logger.debug("#{prefix}: done.") end)
  end

  def record_error(computation, error_details) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [:error]"
    Logger.info("#{prefix}: marking as completed. starting.")

    {:ok, _} =
      Journey.Scheduler.Helpers.transaction_with_deadlock_retry(
        fn repo ->
          Logger.info("#{prefix}: marking as completed. transaction starting.")

          current_computation =
            from(c in Computation, where: c.id == ^computation.id)
            |> repo.one!()

          if current_computation.state == :computing do
            new_revision =
              Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(computation.execution_id, repo)

            # Mark the computation as "failed".
            now_seconds = System.system_time(:second)

            computation
            |> Ecto.Changeset.change(%{
              error_details: "#{inspect(error_details)}" |> String.trim() |> String.slice(0, 1000),
              completion_time: now_seconds,
              updated_at: now_seconds,
              state: :failed,
              ex_revision_at_completion: new_revision
            })
            |> repo.update!()
            |> Journey.Scheduler.Retry.maybe_schedule_a_retry(repo)

            Logger.info("#{prefix}: marking as completed. transaction done.")
          else
            Logger.warning(
              "#{prefix}: computation completed, but it is no longer :computing. (#{current_computation.state})"
            )
          end
        end,
        prefix
      )
      |> case do
        {:ok, result} ->
          {:ok, result}

        {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
          Logger.warning(
            "#{prefix}: Failed after retries due to deadlock, " <>
              "computation will be retried by abandoned sweeper"
          )

          {:ok, nil}

        {:error, other} ->
          Logger.error("#{prefix}: Transaction failed with error: #{inspect(other)}")
          raise other
      end

    Logger.info("#{prefix}: marking as completed. done.")
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

      # record_result(
      #   repo,
      #   graph_node.mutates,
      #   computation.node_name,
      #   computation.execution_id,
      #   new_revision,
      #   result
      # )

      computation.computation_type
      |> case do
        :compute ->
          record_result(
            repo,
            graph_node.mutates,
            false,
            computation.node_name,
            computation.execution_id,
            new_revision,
            result,
            :compute
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
    else
      Logger.warning(
        "#{prefix}: done. computation completed, but it is no longer :computing. (#{current_computation.state})"
      )
    end
  end

  # Compute nodes: only update if value changed (idempotent, like Journey.set/3)
  defp record_result(repo, nil, _update_revision_on_change, node_name, execution_id, new_revision, result, :compute) do
    current_value = get_current_node_value(repo, execution_id, node_name)

    if current_value != result do
      # Value changed - update with new revision to trigger downstream recomputation
      set_value(execution_id, node_name, new_revision, repo, result)
    else
      # Value unchanged - skip update entirely (matching Journey.set/3 behavior)
      Logger.debug("[#{execution_id}] [#{node_name}]: compute node value unchanged, skipping update")
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
  end

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
