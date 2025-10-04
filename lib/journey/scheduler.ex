defmodule Journey.Scheduler do
  @moduledoc false

  alias Journey.Scheduler.Completions

  require Logger
  import Journey.Helpers.Log

  def advance(nil) do
    Logger.warning("[#{mf()}] [#{inspect(self())}] - advancing a nil execution")
    nil
  end

  def advance(execution) do
    prefix = "[#{execution.id}] [#{mf()}] [#{inspect(self())}]"
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

    if length(available_computations) > 0 do
      execution = Journey.load(execution)

      available_computations
      |> Enum.each(fn %{computation: to_compute, fulfilled_conditions: conditions_fulfilled} ->
        launch_computation(execution, to_compute, conditions_fulfilled)
      end)

      Journey.load(execution)
    else
      execution
    end
    |> tap(fn _ -> Logger.debug("#{prefix}: done") end)
  end

  defp launch_computation(execution, computation, conditions_fulfilled) do
    computation_params = execution |> Journey.values(reload: false)

    # Start the computation in a separate process, as a "fire-and-forget" task.
    # Note that this task is intentionally not OTP-"supervised" – we are using
    # database-based supervision instead.
    Task.start(fn ->
      prefix = "[#{execution.id}.#{computation.node_name}.#{computation.id}] [#{mf()}] [#{execution.graph_name}]"
      Logger.debug("#{prefix}: starting async computation")

      graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
      graph_node = Journey.Graph.find_node_by_name(graph, computation.node_name)

      input_versions_to_capture =
        conditions_fulfilled
        |> Enum.map(fn %{upstream_node: v} -> {v.node_name, v.ex_revision} end)
        |> Enum.into(%{})

      r =
        try do
          # Build value nodes map from upstream nodes
          value_nodes_map = build_value_nodes_map(conditions_fulfilled)

          # Introspect arity and call accordingly
          case Function.info(graph_node.f_compute)[:arity] do
            1 ->
              graph_node.f_compute.(computation_params)

            2 ->
              graph_node.f_compute.(computation_params, value_nodes_map)

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

      r
      |> case do
        {:ok, result} ->
          Logger.debug("#{prefix}: async computation completed successfully")
          Completions.record_success(computation, input_versions_to_capture, result)

        {:error, error_details} ->
          Logger.warning("#{prefix}: async computation completed with an error")
          Completions.record_error(computation, error_details)
          jitter_ms = :rand.uniform(10_000)
          Process.sleep(jitter_ms)

        unexpected_value ->
          result_truncated = "#{inspect(unexpected_value)}" |> String.trim() |> String.slice(0, 1000)

          Logger.error(
            "#{prefix}: #{computation.node_name}'s f_compute function was expected to return `{:ok, _}` or {:error, _} tuples, but it returned an unexpected value: '#{result_truncated}'"
          )

          Completions.record_error(computation, "Unexpected value: '#{result_truncated}'")
          jitter_ms = :rand.uniform(10_000)
          Process.sleep(jitter_ms)
      end

      invoke_f_on_save(prefix, graph_node.f_on_save, graph.f_on_save, execution.id, computation.node_name, r)

      if requires_invalidation_check?(r, graph_node) do
        # After a compute node succeeds, check if any downstream values should be invalidated
        Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(execution.id, graph)
      end

      advance(execution)
    end)

    execution
  end

  defp invoke_f_on_save(prefix, node_f_on_save, graph_f_on_save, execution_id, node_name, result) do
    # First invoke node-specific f_on_save if defined
    if node_f_on_save do
      Task.start(fn ->
        Logger.debug("#{prefix}: calling node-specific f_on_save")

        try do
          node_f_on_save.(execution_id, result)
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

  defp requires_invalidation_check?({:ok, _result}, graph_node), do: graph_node.type == :compute
  defp requires_invalidation_check?(_other_result, _graph_node), do: false

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
