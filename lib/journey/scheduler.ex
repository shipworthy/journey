defmodule Journey.Scheduler do
  @moduledoc false

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Scheduler.Completions

  require Logger

  def advance(nil) do
    Logger.warning("[#{inspect(self())}] - advancing a nil execution")
    nil
  end

  def advance(execution) do
    prefix = "[#{execution.id}] [#{inspect(self())}]"
    Logger.debug("#{prefix}: starting")

    # Migrate execution to current graph if needed
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)

    # Fetch graph again after potential migration (execution may have updated graph version)
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    advance_with_graph(prefix, execution, graph)
  end

  defp advance_with_graph(prefix, execution, nil) do
    Logger.debug("#{prefix}: missing graph, (#{inspect(execution.graph_name)})")
    execution
  end

  defp advance_with_graph(prefix, execution, graph) do
    prefix = "#{prefix} [advance_with_graph]"
    Logger.debug("#{prefix} starting")
    Journey.Scheduler.Recompute.detect_updates_and_create_re_computations(execution, graph)

    available_computations =
      Journey.Scheduler.Available.grab_available_computations(execution, graph)

    if available_computations != [] do
      execution = Journey.load(execution, computations: [:not_set, :computing])

      available_computations
      |> Enum.each(fn %{computation: to_compute, fulfilled_conditions: conditions_fulfilled} ->
        launch_computation(execution, to_compute, conditions_fulfilled)
      end)

      execution
    else
      execution
    end
    |> tap(fn _ -> Logger.debug("#{prefix}: done") end)
  end

  defp launch_computation(execution, computation, conditions_fulfilled) do
    computation_params = execution |> Journey.values(reload: false)

    # Start the computation in a separate process, as a "fire-and-forget" task.
    # Note that this process is intentionally not OTP-"supervised" – we are using
    # database-based "supervision" instead.
    Task.start(fn ->
      worker_with_heartbeat(execution, computation, computation_params, conditions_fulfilled)
    end)

    execution
  end

  defp worker_with_heartbeat(execution, computation, computation_params, conditions_fulfilled) do
    start_time = System.monotonic_time(:second)
    prefix = "Worker [#{execution.id}.#{computation.id}.#{computation.node_name}] [#{execution.graph_name}]"
    Logger.info("#{prefix}: starting async computation")

    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
    graph_node = Journey.Graph.find_node_by_name(graph, computation.node_name)

    # Spawn linked heartbeat sibling - receives EXIT when worker exits
    _heartbeat_pid =
      spawn_link(fn ->
        Journey.Scheduler.Heartbeat.run(
          execution.id,
          computation.id,
          computation.node_name,
          graph_node.heartbeat_interval_seconds,
          graph_node.heartbeat_timeout_seconds
        )
      end)

    r =
      do_compute(
        prefix,
        execution,
        computation,
        computation_params,
        conditions_fulfilled,
        graph,
        graph_node
      )

    end_time = System.monotonic_time(:second)
    Logger.info("#{prefix}: async computation completed after #{end_time - start_time} seconds")
    r
  end

  defp do_compute(prefix, execution, computation, computation_params, conditions_fulfilled, graph, graph_node) do
    input_versions_to_capture =
      conditions_fulfilled
      |> Enum.map(fn %{upstream_node: v} -> {v.node_name, v.ex_revision} end)
      |> Enum.into(%{})

    r =
      try do
        # Build value nodes map from upstream nodes
        value_nodes_map = build_value_nodes_map(conditions_fulfilled)

        # For :loop nodes, inject the previous iteration's :cont_* payload as values.<name>.
        # This is the loop's "self-reference" — read from the computations table, not the values table.
        params =
          if graph_node.type == :loop do
            inject_loop_self_reference(computation_params, computation)
          else
            computation_params
          end

        # Introspect arity and call accordingly
        case Function.info(graph_node.f_compute)[:arity] do
          1 ->
            graph_node.f_compute.(params)

          2 ->
            graph_node.f_compute.(params, value_nodes_map)

          arity ->
            raise ArgumentError,
                  "f_compute must be arity 1 or 2, got arity #{arity} for node #{computation.node_name}"
        end
      rescue
        e ->
          exception_as_string = Exception.format(:error, e, __STACKTRACE__)

          Logger.error("#{prefix}: f_compute raised an exception, #{Exception.format(:error, e, __STACKTRACE__)}")

          {:error, "Exception. #{exception_as_string}"}
      end

    {r_for_callback, write_outcome} =
      handle_computation_result(r, prefix, computation, graph_node, input_versions_to_capture)

    maybe_invoke_f_on_save(prefix, graph, graph_node, execution, computation, r_for_callback, write_outcome)

    # If the values table was actually written, check whether downstream values should be invalidated.
    # The `:value_written` flag is set by Completions.record_success when set_value/5 ran;
    # for loops it includes both terminal :ok and cap-promoted :cont_with_fallback.
    if write_outcome == :value_written and graph_node.type in [:compute, :historian, :loop] do
      Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(execution.id, graph)
    end

    # Clean up old completed computations if retention is configured.
    # Runs synchronously but is fast at steady state (~1 row deleted).
    if successful_completion?(r) do
      Journey.Scheduler.Retention.maybe_cleanup(execution.id, graph_node, graph)
    end

    advance(execution)
  end

  # Successful-completion test for retention cleanup. For loop nodes, all four return shapes
  # except {:error, _} represent a successful iteration (the iteration ran to completion).
  defp successful_completion?({:ok, _}), do: true
  defp successful_completion?({:cont_with_fallback, _}), do: true
  defp successful_completion?({:cont_no_fallback, _}), do: true
  defp successful_completion?(_), do: false

  # Sourced from Journey.Scheduler.Helpers at compile time so guards below can pattern-match on
  # this list. :schedule_*/:tick_* are out of scope — their f_compute returns a schedule time and
  # they preserve the prior pass-through behavior (fire on every completion).
  @retry_eligible_types Journey.Scheduler.Helpers.retry_eligible_types()

  defp maybe_invoke_f_on_save(prefix, graph, graph_node, execution, computation, r, write_outcome) do
    case build_f_on_save_result(r, write_outcome, graph_node) do
      nil ->
        :ok

      result ->
        invoke_f_on_save(
          prefix,
          graph_node.f_on_save,
          graph.f_on_save,
          execution.id,
          computation.node_name,
          result
        )
    end
  end

  defp build_f_on_save_result(r, write_outcome, %{type: :loop}) do
    case {write_outcome, r} do
      {:value_written, {:ok, value}} -> {:ok, value}
      {:value_written, {:cont_with_fallback, value}} -> {:ok, value}
      {:loop_cap_failed, _} -> {:error, "max_iterations_reached"}
      {:retries_exhausted, {:error, _} = err} -> err
      _ -> nil
    end
  end

  defp build_f_on_save_result({:ok, _} = r, :value_written, %{type: t}) when t in @retry_eligible_types, do: r

  defp build_f_on_save_result({:error, _} = r, :retries_exhausted, %{type: t}) when t in @retry_eligible_types, do: r

  defp build_f_on_save_result(_r, _write_outcome, %{type: t}) when t in @retry_eligible_types, do: nil

  defp build_f_on_save_result(r, _write_outcome, _graph_node), do: r

  # Each clause returns {r_for_callback, write_outcome}. r_for_callback is the value passed to the
  # f_on_save gate; for ok/error/cont_* it equals the original f_compute return, but the unexpected-value
  # clause synthesizes an {:error, _} so callbacks can pattern-match consistently on retry exhaustion.
  #
  # For :loop nodes, accept the four-tuple return contract: :ok / :cont_with_fallback / :cont_no_fallback / :error.
  # For non-loop nodes, only :ok / :error are accepted; everything else falls through to the unexpected-value path.
  defp handle_computation_result({:ok, result} = r, prefix, computation, %{type: :loop}, input_versions_to_capture) do
    Logger.debug("#{prefix}: async loop iteration completed (:ok)")
    {:ok, write_outcome} = Completions.record_success(computation, input_versions_to_capture, {:ok, result})
    {r, write_outcome}
  end

  defp handle_computation_result(
         {:cont_with_fallback, _} = r,
         prefix,
         computation,
         %{type: :loop},
         input_versions_to_capture
       ) do
    Logger.debug("#{prefix}: async loop iteration completed (:cont_with_fallback)")

    {:ok, write_outcome} =
      Completions.record_success(computation, input_versions_to_capture, r)

    {r, write_outcome}
  end

  defp handle_computation_result(
         {:cont_no_fallback, _} = r,
         prefix,
         computation,
         %{type: :loop},
         input_versions_to_capture
       ) do
    Logger.debug("#{prefix}: async loop iteration completed (:cont_no_fallback)")

    {:ok, write_outcome} =
      Completions.record_success(computation, input_versions_to_capture, r)

    {r, write_outcome}
  end

  defp handle_computation_result({:ok, result} = r, prefix, computation, _graph_node, input_versions_to_capture) do
    Logger.debug("#{prefix}: async computation completed successfully")
    {:ok, write_outcome} = Completions.record_success(computation, input_versions_to_capture, result)
    {r, write_outcome}
  end

  defp handle_computation_result(
         {:error, error_details} = r,
         prefix,
         computation,
         _graph_node,
         input_versions_to_capture
       ) do
    Logger.warning("#{prefix}: async computation completed with an error")
    status = Completions.record_error(computation, error_details, input_versions_to_capture)
    jitter_ms = :rand.uniform(10_000)
    Process.sleep(jitter_ms)
    {r, error_status_to_write_outcome(status)}
  end

  defp handle_computation_result(unexpected_value, prefix, computation, _graph_node, input_versions_to_capture) do
    result_truncated = "#{inspect(unexpected_value)}" |> String.trim() |> String.slice(0, 1000)

    Logger.error(
      "#{prefix}: #{computation.node_name}'s f_compute function returned an unexpected value: '#{result_truncated}'"
    )

    error_details = "Unexpected value: '#{result_truncated}'"
    status = Completions.record_error(computation, error_details, input_versions_to_capture)
    jitter_ms = :rand.uniform(10_000)
    Process.sleep(jitter_ms)
    # Synthesize an {:error, _} so the callback gets a useful payload at retry exhaustion.
    {{:error, error_details}, error_status_to_write_outcome(status)}
  end

  # Maps record_error's :retried | :exhausted | :no_state_change return to a write_outcome the
  # f_on_save gate can pattern-match on. Only :exhausted maps to :retries_exhausted, which fires
  # the callback once with the original {:error, _} shape via build_f_on_save_result/3.
  defp error_status_to_write_outcome(:exhausted), do: :retries_exhausted
  defp error_status_to_write_outcome(_), do: :no_value_written

  @doc false
  def invoke_f_on_save(prefix, node_f_on_save, graph_f_on_save, execution_id, node_name, result) do
    # First invoke node-specific f_on_save if defined
    if node_f_on_save do
      Task.start(fn ->
        Logger.debug("#{prefix}: calling node-specific f_on_save")

        try do
          case node_f_on_save do
            f when is_function(f, 3) -> f.(execution_id, node_name, result)
            f when is_function(f, 2) -> f.(execution_id, result)
          end
        rescue
          e ->
            Logger.error("#{prefix}: node-specific f_on_save raised an exception: '#{inspect(e)}'")
        end

        Logger.debug("#{prefix}: node-specific f_on_save completed")
      end)
    end

    # Then invoke graph-wide f_on_save if defined
    if graph_f_on_save do
      Task.start(fn ->
        Logger.debug("#{prefix}: calling graph-wide f_on_save")

        try do
          graph_f_on_save.(execution_id, node_name, result)
        rescue
          e ->
            Logger.error("#{prefix}: graph-wide f_on_save raised an exception: '#{inspect(e)}'")
        end

        Logger.debug("#{prefix}: graph-wide f_on_save completed")
      end)
    end

    if is_nil(node_f_on_save) and is_nil(graph_f_on_save) do
      Logger.debug("#{prefix}: no f_on_save defined (neither node-specific nor graph-wide)")
    end
  end

  # Sets `values_map.<name>` to the loop's previous-iteration :cont_* payload, or removes the
  # entry entirely if there is no previous iteration in the current run.
  #
  # The clear-then-inject shape is load-bearing. The values table may carry a settled value from
  # a previous run that terminated with :ok or cap-promotion; without `Map.delete/2`, iter 1 of a
  # fresh run would silently inherit it, breaking the documented "iter 1 sees values.<name> as
  # unset" contract. Clearing first makes iter 1's view structurally correct regardless of what
  # the values table holds; iter 2+ then re-injects from the computations table.
  defp inject_loop_self_reference(computation_params, computation) do
    cleared = Map.delete(computation_params, computation.node_name)

    case fetch_previous_loop_value(
           computation.execution_id,
           computation.node_name,
           computation.loop_iteration
         ) do
      :none -> cleared
      {:ok, value} -> Map.put(cleared, computation.node_name, value)
    end
  end

  # Returns `:none | {:ok, value}`. The tagged shape is load-bearing: it lets the caller
  # distinguish "no previous-iteration row" from "row exists, carried value is nil." Without
  # the tag, a step function returning `{:cont_*, nil}` would round-trip as a missing key on
  # the next iteration, contradicting the documented "iter 2+ reflects the most recent :cont_*
  # payload" contract.
  #
  # Iter 1 of any run has no preceding iteration — short-circuit without a DB round-trip.
  # This is also the most common case (every fresh run hits this path).
  defp fetch_previous_loop_value(_execution_id, _node_name, 1), do: :none
  defp fetch_previous_loop_value(_execution_id, _node_name, nil), do: :none

  defp fetch_previous_loop_value(execution_id, node_name, current_iter) when is_atom(node_name) do
    fetch_previous_loop_value(execution_id, Atom.to_string(node_name), current_iter)
  end

  # Filters by `loop_iteration = current_iter - 1` and orders by `ex_revision_at_completion DESC`
  # to isolate runs from each other.
  #
  # Invariant: ex_revision_at_completion is monotonic per execution. When multiple runs of this
  # loop have left :success rows at the same iteration number, the current run's row always has
  # the largest completion revision — Run B iter 1 is only inserted strictly after Run A reaches
  # a terminal state (Recompute's WHERE NOT EXISTS check, plus the invariant that Invalidate only
  # creates a fresh iter 1 when no continuation is in flight), and revisions are atomically
  # incremented per execution by Helpers.increment_execution_revision_in_transaction/2.
  #
  # Without this isolation, a fresh Run B iter 1 would silently inherit Run A's last :cont_*
  # payload as its self-reference, breaking the documented "iter 1 sees values.<name> as unset"
  # contract.
  defp fetch_previous_loop_value(execution_id, node_name, current_iter)
       when is_binary(node_name) and is_integer(current_iter) and current_iter > 1 do
    case execution_id
         |> previous_loop_state_query(node_name, current_iter - 1)
         |> Journey.Repo.one() do
      nil -> :none
      loop_state -> extract_loop_state_value(loop_state)
    end
  end

  defp previous_loop_state_query(execution_id, node_name, previous_iter) do
    from(c in Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^node_name and
          c.computation_type == ^:loop and
          c.state == ^:success and
          c.loop_iteration == ^previous_iter and
          not is_nil(c.loop_state),
      order_by: [desc: c.ex_revision_at_completion],
      limit: 1,
      select: c.loop_state
    )
  end

  defp extract_loop_state_value(%{"value" => value}), do: {:ok, value}
  defp extract_loop_state_value(_), do: :none

  defp build_value_nodes_map(conditions_fulfilled) do
    conditions_fulfilled
    |> Enum.map(fn %{upstream_node: value_node} ->
      {value_node.node_name,
       %{
         node_value: value_node.node_value,
         metadata: value_node.metadata,
         revision: value_node.ex_revision,
         set_time: value_node.set_time
       }}
    end)
    |> Enum.into(%{})
  end
end
