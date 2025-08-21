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
    advance_with_graph(prefix, execution, Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version))
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

      graph_node =
        Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
        |> Journey.Graph.find_node_by_name(computation.node_name)

      input_versions_to_capture =
        conditions_fulfilled
        |> Enum.map(fn %{upstream_node: v} -> {v.node_name, v.ex_revision} end)
        |> Enum.into(%{})

      r =
        try do
          graph_node.f_compute.(computation_params)
        rescue
          e ->
            exception_as_string = Exception.format(:error, e, __STACKTRACE__)
            Logger.error("#{prefix}: f_compute raised an exception, #{Exception.format(:error, e, __STACKTRACE__)}")
            {:error, "Exception. #{exception_as_string}"}
        end

      should_check_invalidation =
        r
        |> case do
          {:ok, result} ->
            Logger.debug("#{prefix}: async computation completed successfully")
            Completions.record_success(computation, input_versions_to_capture, result)
            # Only check invalidation for compute nodes (not mutate/schedule nodes)
            graph_node.type == :compute

          {:error, error_details} ->
            Logger.warning("#{prefix}: async computation completed with an error")
            Completions.record_error(computation, error_details)
            jitter_ms = :rand.uniform(10_000)
            Process.sleep(jitter_ms)
            false

          unexpected_value ->
            result_truncated = "#{inspect(unexpected_value)}" |> String.trim() |> String.slice(0, 1000)

            Logger.error(
              "#{prefix}: #{computation.node_name}'s f_compute function was expected to return `{:ok, _}` or {:error, _} tuples, but it returned an unexpected value: '#{result_truncated}'"
            )

            Completions.record_error(computation, "Unexpected value: '#{result_truncated}'")
            jitter_ms = :rand.uniform(10_000)
            Process.sleep(jitter_ms)
            false
        end

      invoke_f_on_save(prefix, graph_node.f_on_save, execution.id, r)

      if should_check_invalidation do
        # After a compute node succeeds, check if any downstream values should be invalidated
        updated_execution = Journey.load(execution.id)
        updated_execution = Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution)
        advance(updated_execution)
      else
        advance(execution)
      end
    end)

    execution
  end

  defp invoke_f_on_save(prefix, nil, _eid, _result) do
    Logger.debug("#{prefix}: f_on_save is not defined, skipping")
    nil
  end

  defp invoke_f_on_save(prefix, f, eid, result) do
    Task.start(fn ->
      Logger.debug("#{prefix}: calling f_on_save")

      try do
        f.(eid, result)
      rescue
        e ->
          Logger.error("#{prefix}: f_on_save raised an exception: '#{inspect(e)}'")
      end

      Logger.debug("#{prefix}: f_on_save completed")
    end)
  end
end
