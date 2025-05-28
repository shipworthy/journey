defmodule Journey.Scheduler.Recompute do
  @moduledoc false

  import Ecto.Query
  require Logger

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph

  import Journey.Helpers.Log
  import Journey.Node.UpstreamDependencies.Computations, only: [unblocked?: 2]

  def detect_updates_and_create_re_computations(execution, graph) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.debug("#{prefix}: starting")

    {:ok, new_computations} =
      Journey.Repo.transaction(fn repo ->
        latest_computation_ids =
          from(c in Computation,
            where: c.execution_id == ^execution.id and c.computation_type == :compute and c.state != ^:not_set,
            order_by: [desc: c.ex_revision_at_start],
            distinct: c.node_name,
            select: c.id
          )

        all_computations =
          from(c in Computation,
            where: c.id in subquery(latest_computation_ids),
            # TODO: here and elsewhere, experiment with a regular "SELECT FOR UPDATE", no "SKIP LOCKED".
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
    gated_by =
      graph
      |> Graph.find_node_by_name(computation.node_name)
      |> Map.get(:gated_by)
      |> Journey.Node.UpstreamDependencies.Computations.list_all_node_names()

    current_node_revisions =
      all_values
      |> Enum.filter(fn %{node_name: node_name} -> node_name in gated_by end)
      |> Enum.map(fn %{node_name: node_name} = node -> {node_name, node.ex_revision} end)
      |> Enum.into(%{})

    computed_with_node_revisions =
      computation.computed_with
      |> case do
        nil ->
          %{}

        node_and_revisions ->
          node_and_revisions
          |> Enum.map(fn {k, v} ->
            {String.to_atom(k), v}
          end)
          |> Enum.into(%{})
      end

    any_new_versions? =
      computed_with_node_revisions
      |> Enum.any?(fn {upstream_node_name, computed_with_revision} ->
        current_node_revisions[upstream_node_name] != nil and
          current_node_revisions[upstream_node_name] > computed_with_revision
      end)

    any_new_versions?
  end
end
