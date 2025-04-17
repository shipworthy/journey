defmodule Journey.Scheduler.Available do
  @moduledoc false

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph

  require Logger
  import Journey.Helpers.Log

  def grab_available_computations(execution, nil) when is_struct(execution, Execution) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.error("#{prefix}: unknown graph #{execution.graph_name}")
    []
  end

  def grab_available_computations(execution, graph)
      when is_struct(execution, Execution) and is_struct(graph, Journey.Graph) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.info("#{prefix}: grabbing available computation")

    {:ok, computations_to_perform} =
      Journey.Repo.transaction(fn repo ->
        all_candidates_for_computation =
          from(c in Computation,
            where:
              c.execution_id == ^execution.id and
                c.state == ^:not_set and
                c.computation_type in [^:compute, ^:pulse_once, ^:pulse_recurring],
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()
          |> Journey.Executions.convert_values_to_atoms(:node_name)

        all_set_values =
          from(v in Value,
            where: v.execution_id == ^execution.id and not is_nil(v.set_time)
          )
          |> repo.all()
          |> Enum.filter(fn
            %{node_type: :compute} ->
              true

            %{node_type: :pulse_once, node_value: %{"v" => enabled_at}} ->
              System.system_time(:second) >= enabled_at

            %{node_type: :pulse_recurring, node_value: %{"v" => enabled_at}} ->
              System.system_time(:second) >= enabled_at

            %{node_type: :input} ->
              true
          end)
          |> Enum.map(fn %{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)
          |> Enum.map(fn %{node_name: node_name} -> node_name end)
          |> MapSet.new()

        all_candidates_for_computation
        |> Enum.filter(fn computation -> upstream_dependencies_fulfilled?(graph, computation, all_set_values) end)
        |> Enum.map(fn unblocked_computation ->
          grab_this_computation(graph, execution, unblocked_computation, repo)
        end)
      end)

    selected_computation_names =
      computations_to_perform
      |> Enum.map_join(", ", fn computation -> computation.node_name end)
      |> String.trim()

    Logger.info("#{prefix}: selected these computations: [#{selected_computation_names}]")

    computations_to_perform
  end

  defp grab_this_computation(graph, execution, computation, repo) do
    # Increment revision on the execution.
    {1, [new_revision]} =
      from(e in Execution, update: [inc: [revision: 1]], where: e.id == ^execution.id, select: e.revision)
      |> repo.update_all([])

    # Mark the computation as "computing".
    graph_node = Graph.find_node_by_name(graph, computation.node_name)

    computation
    |> Ecto.Changeset.change(%{
      state: :computing,
      start_time: System.system_time(:second),
      ex_revision_at_start: new_revision,
      deadline: System.system_time(:second) + graph_node.abandon_after_seconds
    })
    |> repo.update!()
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
