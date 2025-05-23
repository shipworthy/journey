defmodule Journey.Scheduler.Available do
  @moduledoc false

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph
  alias Journey.Node.UpstreamDependencies

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
    Logger.debug("#{prefix}: grabbing available computation")

    {:ok, computations_to_perform} =
      Journey.Repo.transaction(fn repo ->
        all_candidates_for_computation =
          from(c in Computation,
            where:
              c.execution_id == ^execution.id and
                c.state == ^:not_set and
                c.computation_type in [^:compute, ^:schedule_once, ^:schedule_recurring],
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()
          |> Journey.Executions.convert_values_to_atoms(:node_name)

        all_value_nodes = Journey.Execution.Values.load_from_db(execution.id, repo)

        all_candidates_for_computation
        |> Enum.map(fn computation_candidate ->
          gated_by =
            graph
            |> Graph.find_node_by_name(computation_candidate.node_name)
            |> Map.get(:gated_by)

          UpstreamDependencies.Computations.evaluate_computation_for_readiness(all_value_nodes, gated_by)
          |> Map.put(:computation, computation_candidate)
        end)
        |> Enum.map(fn c ->
          summary =
            Journey.Scheduler.Introspection.readiness_state(
              c.ready?,
              c.conditions_met,
              c.conditions_not_met,
              c.computation.node_name
            )

          Logger.info("#{prefix}: v0 [#{inspect(c.computation.node_name)}]\n=====\n#{summary}\n=====")
          c
        end)
        |> Enum.filter(fn %{ready?: ready?} -> ready? end)
        |> Enum.map(fn %{ready?: true, computation: unblocked_computation, conditions_met: fulfilled_conditions} ->
          %{
            computation: grab_this_computation(graph, execution, unblocked_computation, repo),
            fulfilled_conditions: fulfilled_conditions
          }
        end)
      end)

    selected_computation_names =
      computations_to_perform
      |> Enum.map_join(", ", fn %{computation: computation, fulfilled_conditions: _fulfilled_upstream_dependencies} ->
        computation.node_name
      end)
      |> String.trim()

    if selected_computation_names == "" do
      Logger.debug("#{prefix}: no computations")
    else
      Logger.info("#{prefix}: selected these computations: [#{selected_computation_names}]")
    end

    computations_to_perform
  end

  def evaluate({value_node, f_condition}) when is_struct(value_node, Value) and is_function(f_condition, 1) do
    if f_condition.(value_node) do
      {true, [value_node.node_name]}
    else
      {false, [value_node.node_name]}
    end
  end

  defp grab_this_computation(graph, execution, computation, repo) do
    # Increment revision on the execution.
    new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution.id, repo)

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
end
