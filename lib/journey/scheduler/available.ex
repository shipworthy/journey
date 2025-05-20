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

        all_value_nodes =
          from(v in Value,
            where: v.execution_id == ^execution.id
          )
          |> repo.all()
          |> Enum.map(fn %{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)

        all_candidates_for_computation
        |> Enum.map(fn computation_candidate ->
          Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
            all_value_nodes,
            graph
            |> Graph.find_node_by_name(computation_candidate.node_name)
            |> Map.get(:upstream_nodes)
          )
          |> Map.put(:computation, computation_candidate)

          # |> case do
          #   %{ready?: true, conditions_met: conditions_met} = c ->
          #     Logger.info(
          #       "#{prefix}: computation ready: #{inspect(computation_candidate)}. conditions met: #{inspect(conditions_met)}"
          #     )

          #     c

          #   %{ready?: false, conditions_not_met: conditions_not_met} = c ->
          #     Logger.info(
          #       "#{prefix}: computation not ready: #{inspect(computation_candidate)}. conditions not met: #{inspect(conditions_not_met)}"
          #     )

          #     c
          # end
        end)
        |> Enum.map(fn
          %{ready?: true, conditions_met: conditions_met} = c ->
            Logger.info(
              "#{prefix}: computation ready: #{inspect(c.computation)}. conditions met: #{inspect(conditions_met)}"
            )

            c

          %{ready?: false, conditions_not_met: conditions_not_met} = c ->
            Logger.info(
              "#{prefix}: computation not ready: #{inspect(c.computation)}. conditions not met: #{inspect(conditions_not_met)}"
            )

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

  # defp evaluate_computation_for_readiness(
  #        all_executions_values,
  #        {:and, {upstream_node_name1, f_condition1}, {upstream_node_name2, f_condition2}} = upstream_conditions
  #      ) do
  #   IO.inspect(all_executions_values, label: "all_executions_values")
  #   IO.inspect(upstream_conditions, label: "upstream_conditions")
  #   # eval_result = evaluate({%Value{node_name: :first_name}, &mario?/1})

  #   # relevant_value_node =
  #   #   all_executions_values
  #   #   |> Enum.find(fn %{node_name: node_name} -> node_name == upstream_node_name end)
  #   #   |> IO.inspect(label: "relevant_value_node")

  #   # if relevant_value_node == nil do
  #   #   raise "missing value node for #{upstream_node_name}"
  #   # end

  #   # if f_condition.(relevant_value_node) do
  #   #   %{ready?: true, conditions_met: [%{upstream_node: relevant_value_node, f_condition: f_condition}]}
  #   # else
  #   %{ready?: false, conditions_not_met: [%{upstream_node: upstream_node_name1, f_condition: f_condition1}]}
  #   # end
  # end

  # defp evaluate_computation_for_readiness(
  #        all_executions_values,
  #        {upstream_node_name, f_condition} = upstream_conditions
  #      ) do
  #   IO.inspect(all_executions_values, label: "all_executions_values")
  #   IO.inspect(upstream_conditions, label: "upstream_conditions")
  #   # eval_result = evaluate({%Value{node_name: :first_name}, &mario?/1})

  #   relevant_value_node =
  #     all_executions_values
  #     |> Enum.find(fn %{node_name: node_name} -> node_name == upstream_node_name end)
  #     |> IO.inspect(label: "relevant_value_node")

  #   if relevant_value_node == nil do
  #     raise "missing value node for #{upstream_node_name}"
  #   end

  #   if f_condition.(relevant_value_node) do
  #     %{ready?: true, conditions_met: [%{upstream_node: relevant_value_node, f_condition: f_condition}]}
  #   else
  #     %{ready?: false, conditions_not_met: [%{upstream_node: relevant_value_node, f_condition: f_condition}]}
  #   end
  # end

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
