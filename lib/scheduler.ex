defmodule Journey.Scheduler do
  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph

  require Logger
  import Journey.Helpers.Log

  def advance(execution) do
    available_computations = grab_available_computations(execution)

    if length(available_computations) > 0 do
      execution = Journey.load(execution)

      available_computations
      |> Enum.each(fn to_compute -> schedule_computation(execution, to_compute) end)

      # TODO: do a bit of load / performance testing to see if we get any benefit from advancing
      # the execution here. (Until then, since we have just examined all candidates for computation, this seems unlikely
      # to provide any benefit.)
      # advance(execution)
      Journey.load(execution)
    else
      execution
    end
  end

  defp schedule_computation(execution, computation) do
    # Here we would update the execution with the scheduled computation
    # For example, we could set the start time and state of the computation
    # execution = %{execution | scheduled_computation: computation}

    Task.start(fn ->
      nil

      Logger.info("[#{execution.id}][#{computation.node_name}] [#{mf()}] starting async computation")

      # compute(node, execution)
      # TODO: call the function, process the result, update the store.
      Logger.info("[#{execution.id}][#{computation.node_name}] [#{mf()}] completed async computation")
    end)

    execution
  end

  defp grab_available_computations(execution) do
    Logger.info("[#{execution.id}] [#{mf()}] grabbing available computation")

    graph = Journey.Graph.Catalog.fetch!(execution.graph_name)

    {:ok, computations_to_perform} =
      Journey.Repo.transaction(fn repo ->
        all_candidates_for_computation =
          from(c in Computation,
            where: c.execution_id == ^execution.id and c.state == ^:not_set and c.computation_type == ^:compute,
            # TODO: do a bit of performance / load testing to assess the impact of "SKIP LOCKED".
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()

        all_set_values =
          from(v in Value,
            where: v.execution_id == ^execution.id and not is_nil(v.set_time),
            select: v.node_name
          )
          |> repo.all()
          |> Enum.map(fn node_name -> String.to_atom(node_name) end)
          |> MapSet.new()

        all_candidates_for_computation
        |> Journey.Executions.convert_values_to_atoms(:node_name)
        |> Enum.filter(fn computation -> upstream_dependencies_fulfilled?(graph, computation, all_set_values) end)
        |> Enum.map(fn unblocked_computation ->
          # Increment revision on the execution.
          {1, [new_revision]} =
            from(e in Execution, update: [inc: [revision: 1]], where: e.id == ^execution.id, select: e.revision)
            |> repo.update_all([])

          # Mark the computation as "computing".
          graph_node = Graph.find_node_by_name(graph, unblocked_computation.node_name)

          unblocked_computation
          |> Ecto.Changeset.change(%{
            state: :computing,
            start_time: System.system_time(:second),
            ex_revision_at_start: new_revision,
            deadline: System.system_time(:second) + graph_node.abandon_after_seconds
          })
          |> repo.update!()
        end)
      end)

    selected_computation_names =
      computations_to_perform
      |> Enum.map(fn computation -> computation.node_name end)
      |> Enum.join(", ")
      |> String.trim()

    Logger.info("[#{execution.id}] [#{mf()}] selected these computations: [#{selected_computation_names}]")

    computations_to_perform
  end

  defp upstream_dependencies_fulfilled?(graph, computation, all_set_values) do
    graph
    |> Graph.find_node_by_name(computation.node_name)
    |> Map.get(:upstream_nodes)
    |> Enum.all?(fn upstream_node_name ->
      MapSet.member?(all_set_values, upstream_node_name)
    end)
  end
end
