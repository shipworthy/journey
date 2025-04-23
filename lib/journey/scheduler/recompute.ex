defmodule Journey.Scheduler.Recompute do
  @moduledoc false

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph

  require Logger
  import Journey.Helpers.Log

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
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()
          |> Journey.Executions.convert_values_to_atoms(:node_name)

        all_set_values = get_all_set_values(execution.id, repo)

        all_computations
        |> Enum.filter(fn c -> an_upstream_node_has_a_newer_version?(c, graph, all_set_values) end)
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

  defp get_all_set_values(execution_id, repo) do
    now = System.system_time(:second)
    yesterday = now - 24 * 60 * 60

    from(v in Value,
      where:
        v.execution_id == ^execution_id and not is_nil(v.set_time) and
          (v.node_type == :compute or v.node_type == :input or
             (v.node_type == :pulse_once and fragment("CAST(?->>'v' AS INTEGER) < ?", v.node_value, ^now) and
                fragment("CAST(?->>'v' AS INTEGER) > ?", v.node_value, ^yesterday))),
      select: %{
        node_name: v.node_name,
        node_revision: v.ex_revision,
        node_value: v.node_value,
        set_time: v.set_time
      }
    )
    |> repo.all()
    |> Enum.map(fn %{node_name: node_name} = n ->
      node_name_as_atom = String.to_atom(node_name)
      {node_name_as_atom, %{n | node_name: String.to_atom(node_name)}}
    end)
    |> Enum.into(%{})
  end

  defp an_upstream_node_has_a_newer_version?(computation, graph, all_computed_values) do
    upstream_nodes =
      graph
      |> Graph.find_node_by_name(computation.node_name)
      |> Map.get(:upstream_nodes)

    all_upstream_nodes_have_values? =
      upstream_nodes
      |> Enum.all?(fn upstream_node_name ->
        Map.has_key?(all_computed_values, upstream_node_name)
      end)

    at_least_one_upstream_node_has_a_higher_version? =
      upstream_nodes
      |> Enum.any?(fn upstream_node_name ->
        Map.has_key?(all_computed_values, upstream_node_name) and
          computation.ex_revision_at_start <= all_computed_values[upstream_node_name].node_revision
      end)

    all_upstream_nodes_have_values? and at_least_one_upstream_node_has_a_higher_version?
  end
end
