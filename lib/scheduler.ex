defmodule Journey.Scheduler do
  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value

  require Logger
  import Journey.Helpers.Log

  def advance(execution) do
    available_computation =
      grab_available_computation(execution)
      |> IO.inspect(label: "Available Computation")

    if length(available_computation) > 0 do
      execution = Journey.load(execution)
      schedule_computation(execution, available_computation)
      advance(execution)
    else
      execution
    end
  end

  defp schedule_computation(execution, _computation) do
    # Here we would update the execution with the scheduled computation
    # For example, we could set the start time and state of the computation
    # execution = %{execution | scheduled_computation: computation}

    Task.start(fn ->
      nil
      # Logger.info("[#{execution.id}] [#{mf()}] starting async computation [#{node.name}]")
      # compute(node, execution)
      # TODO: call the function, process the result, update the store.
      # Logger.info("[#{execution.id}] [#{mf()}] completed async computation [#{node.name}]")
    end)

    execution
  end

  defp grab_available_computation(execution) do
    Logger.info("[#{execution.id}] [#{mf()}] grabbing available computation")
    # TODO: implement.
    graph = Journey.Graph.Catalog.fetch!(execution.graph_name)

    {:ok, computations_to_perform} =
      Journey.Repo.transaction(fn repo ->
        # TODO: Increment revision on the execution.

        all_set_values =
          from(v in Value,
            # left_join: c in Computation,
            # on: v.execution_id == c.execution_id and v.node_name == c.node_name,
            where: v.execution_id == ^execution.id and not is_nil(v.set_time),
            # lock: "FOR UPDATE OF v0, c1",
            # limit: 10,
            select: v.node_name
          )
          |> repo.all()
          |> Enum.map(fn node_name ->
            String.to_atom(node_name)
          end)
          |> MapSet.new()

        # |> IO.inspect(label: "omg Values")

        # all_values

        from(c in Journey.Execution.Computation,
          where: c.execution_id == ^execution.id and c.state == ^:not_set and c.computation_type == ^:compute,
          lock: "FOR UPDATE SKIP LOCKED"
          # limit: 1
        )
        |> repo.all()
        |> Journey.Executions.convert_values_to_atoms(:node_name)
        |> Enum.filter(fn computation ->
          # Are all upstream dependency notes set?
          graph_node =
            (graph.inputs_and_steps ++ graph.mutations)
            |> Enum.find(fn n -> n.name == computation.node_name end)

          Enum.all?(graph_node.upstream_nodes, fn upstream_node_name ->
            MapSet.member?(all_set_values, upstream_node_name)
          end)
        end)
        # |> IO.inspect(label: "Available Computations")
        |> Enum.map(fn available_computation ->
          # Increment revision on the execution.
          {1, [new_revision]} =
            from(e in Execution, update: [inc: [revision: 1]], where: e.id == ^execution.id, select: e.revision)
            |> repo.update_all([])

          graph_node =
            (graph.inputs_and_steps ++ graph.mutations)
            |> Enum.find(fn n -> n.name == available_computation.node_name end)

          available_computation
          |> Ecto.Changeset.change(%{
            state: :computing,
            start_time: System.system_time(:second),
            ex_revision_at_start: new_revision,
            deadline: System.system_time(:second) + graph_node.abandon_after_seconds
          })
          |> repo.update!()
        end)

        # |> IO.inspect(label: "Updated Computations")
      end)

    selected_computation_names =
      computations_to_perform
      |> Enum.map(fn computation ->
        computation.node_name
      end)
      |> Enum.join(", ")
      |> String.trim()

    Logger.info("[#{execution.id}] [#{mf()}] selected these computations: [#{selected_computation_names}]")

    computations_to_perform
  end
end
