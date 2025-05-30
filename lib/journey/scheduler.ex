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
    advance_with_graph(prefix, execution, Journey.Graph.Catalog.fetch!(execution.graph_name))
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

    Task.start(fn ->
      prefix = "[#{execution.id}.#{computation.node_name}.#{computation.id}] [#{mf()}] [#{execution.graph_name}]"
      Logger.debug("#{prefix}: starting async computation")

      graph_node =
        execution.graph_name
        |> Journey.Graph.Catalog.fetch!()
        |> Journey.Graph.find_node_by_name(computation.node_name)

      input_versions_to_capture =
        conditions_fulfilled
        |> Enum.map(fn %{upstream_node: v} -> {v.node_name, v.ex_revision} end)
        |> Enum.into(%{})

      recurring_upstream_schedules_to_reschedule =
        find_things_to_reschedule(computation.computed_with, input_versions_to_capture)

      r =
        try do
          graph_node.f_compute.(computation_params)
        rescue
          e ->
            Logger.error("#{prefix}: f_compute raised an exception, #{inspect(e)}")
            {:error, "Exception. #{inspect(e)}"}
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
      end

      invoke_f_on_save(prefix, graph_node.f_on_save, execution.id, r)

      # TODO: how do we make sure rescheduling does not fall through cracks if something fails along the way?
      # TODO: perform the reschedule as part of the same transaction as marking the computation as completed.
      reschedule_recurring(recurring_upstream_schedules_to_reschedule)

      advance(execution)

      # TODO: consider killing the computation after deadline (since we are likely to
      # start other instances of the computation, doing this sounds like a good idea).
      # This could probably be as simple as some version of starting the computation as linked to this process,
      # and exiting the parent after the deadline or when the computation completes, whichever comes first.
      #
      # t = Task.async(fn ->  node.f_compute.(params)  end)
      # Task.await(t, abandoned_after)
      # pros: no lingering tasks. cons: abrupt termination, extra logic.
    end)

    execution
  end

  defp invoke_f_on_save(_prefix, nil, _eid, _result) do
    nil
  end

  defp invoke_f_on_save(prefix, f, eid, result) do
    Task.start(fn ->
      Logger.debug("#{prefix}: calling f_on_save")

      try do
        f.(eid, result)
      rescue
        e ->
          Logger.error("#{prefix}: f_on_save failed, it raised an exception: '#{inspect(e)}'")
      end

      Logger.debug("#{prefix}: f_on_save completed")
    end)
  end

  defp find_things_to_reschedule(original_computation_input_versions, new_computation_input_versions)
       when (original_computation_input_versions == nil or is_map(original_computation_input_versions)) and
              is_map(new_computation_input_versions) do
    # return the list of input nodes whose versions have changed, which are schedule_recurring
    []
  end

  defp reschedule_recurring(recurring_upstream_schedules_to_reschedule)
       when is_list(recurring_upstream_schedules_to_reschedule) do
    # create new computations for each of the schedule_recurring nodes identified in
    # recurring_upstream_schedules_to_reschedule
  end
end
