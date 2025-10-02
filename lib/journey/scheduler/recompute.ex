defmodule Journey.Scheduler.Recompute do
  @moduledoc false

  import Ecto.Query
  require Logger

  alias Journey.Graph
  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  import Journey.Helpers.Log
  import Journey.Node.UpstreamDependencies.Computations, only: [unblocked?: 2]

  def detect_updates_and_create_re_computations(execution, graph) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.debug("#{prefix}: starting")

    {:ok, new_computations} =
      Journey.Repo.transaction(fn repo ->
        latest_computation_ids =
          from(c in Computation,
            where:
              c.execution_id == ^execution.id and
                c.computation_type in [:compute, :mutate, :schedule_once] and c.state == ^:success,
            order_by: [desc: c.ex_revision_at_start],
            distinct: c.node_name,
            select: c.id
          )

        all_computations =
          from(c in Computation,
            where: c.id in subquery(latest_computation_ids),
            lock: "FOR UPDATE"
          )
          |> repo.all()
          |> Journey.Executions.convert_values_to_atoms(:node_name)

        all_values = get_all_values(execution.id, repo)

        all_computations
        # credo:disable-for-lines:10 Credo.Check.Refactor.FilterFilter
        |> Enum.filter(fn c -> an_upstream_node_has_a_newer_version?(c, graph, all_values) end)
        |> Enum.filter(fn c -> unblocked?(all_values, Graph.find_node_by_name(graph, c.node_name).gated_by) end)
        |> Enum.map(fn computation_to_re_create ->
          new_computation =
            %Execution.Computation{
              execution: execution,
              node_name: Atom.to_string(computation_to_re_create.node_name),
              computation_type: computation_to_re_create.computation_type,
              state: :not_set
            }
            |> repo.insert!()

          Logger.info(
            "#{prefix}: created a new re-computation, #{new_computation.id}.#{new_computation.node_name}. an upstream node has a newer version"
          )

          new_computation
        end)
      end)

    if new_computations == [] do
      Logger.debug("#{prefix}: completed. no new re-computations to create")
    else
      Logger.info("#{prefix}: completed. created #{length(new_computations)} new computations")
    end
  end

  defp get_all_values(execution_id, repo) do
    from(v in Value,
      where: v.execution_id == ^execution_id
    )
    |> repo.all()
    |> Enum.map(fn %{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)
  end

  defp an_upstream_node_has_a_newer_version?(computation, graph, all_values) do
    graph_node = Graph.find_node_by_name(graph, computation.node_name)

    # Evaluate which conditions are currently met
    readiness_result =
      Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
        all_values,
        graph_node.gated_by
      )

    if readiness_result.ready? do
      # Extract nodes whose conditions are currently satisfied
      current_node_revisions =
        readiness_result.conditions_met
        |> Enum.map(fn condition_data ->
          {condition_data.upstream_node.node_name, condition_data.upstream_node.ex_revision}
        end)
        |> Enum.into(%{})

      # Compute nodes used to compute the previous value, if any
      computed_with_node_revisions = atomize_map_keys(computation.computed_with)

      # Do any nodes used for previous computation have a newer revision?
      any_updated_versions? =
        computed_with_node_revisions
        |> Enum.any?(fn {upstream_node_name, computed_with_revision} ->
          current_node_revisions[upstream_node_name] != nil and
            current_node_revisions[upstream_node_name] > computed_with_revision
        end)

      # Are there any newly met upstream dependencies?
      any_new_nodes? =
        current_node_revisions
        |> Enum.any?(fn {upstream_node_name, _revision} ->
          not Map.has_key?(computed_with_node_revisions, upstream_node_name)
        end)

      any_updated_versions? or any_new_nodes?
    else
      false
    end
  end

  defp atomize_map_keys(nil), do: %{}

  defp atomize_map_keys(m) when is_map(m) do
    m
    |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
    |> Enum.into(%{})
  end
end
