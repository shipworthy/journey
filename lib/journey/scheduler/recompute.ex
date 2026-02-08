defmodule Journey.Scheduler.Recompute do
  @moduledoc false

  import Ecto.Query
  require Logger

  alias Journey.Graph
  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  import Journey.Node.UpstreamDependencies.Computations, only: [unblocked?: 3]

  # Namespace for PostgreSQL advisory locks used to prevent duplicate re-computations
  @advance_lock_namespace 54_321

  def detect_updates_and_create_re_computations(execution, graph) do
    prefix = "[#{execution.id}]"
    Logger.info("#{prefix}: starting")

    {:ok, new_computations} =
      Journey.Scheduler.Helpers.transaction_with_deadlock_retry(
        fn repo ->
          # Serializes concurrent Recompute calls for the same execution. Without this,
          # two transactions could both run the atomic insert below and neither would see
          # the other's uncommitted row (READ COMMITTED), creating duplicates.
          # The atomic insert separately handles races with record_success, which runs
          # in a different transaction that does not acquire this lock.
          lock_key = :erlang.phash2(execution.id)
          repo.query!("SELECT pg_advisory_xact_lock($1, $2)", [@advance_lock_namespace, lock_key])

          latest_computation_ids =
            from(c in Computation,
              where:
                c.execution_id == ^execution.id and
                  c.computation_type in [
                    :compute,
                    :mutate,
                    :historian,
                    :schedule_once,
                    :tick_once,
                    :schedule_recurring,
                    :tick_recurring
                  ] and
                  c.state == :success,
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
          |> Enum.filter(fn c ->
            an_upstream_node_has_a_newer_version?(c, graph, all_values) and
              unblocked?(all_values, Graph.find_node_by_name(graph, c.node_name).gated_by, :computation)
          end)
          |> Enum.flat_map(fn c ->
            atomic_insert_if_no_duplicate(prefix, execution.id, c, repo)
          end)
        end,
        prefix
      )
      |> case do
        {:ok, computations} ->
          Logger.info("#{prefix}: transaction completed")
          {:ok, computations}

        {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
          Logger.warning(
            "#{prefix}: Failed after retries due to deadlock, " <>
              "will retry on next advance/1 call"
          )

          {:ok, []}

        {:error, other} ->
          Logger.error("#{prefix}: Transaction failed with error: #{inspect(other)}")
          raise other
      end

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
    |> Enum.map(fn %Value{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)
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

  # Atomic conditional insert: creates a re-computation only if no pending
  # (:not_set/:computing) or newer :success computation exists for this node.
  # Using INSERT...SELECT...WHERE NOT EXISTS ensures the check and insert happen
  # in a single SQL statement, which sees one consistent READ COMMITTED snapshot.
  defp atomic_insert_if_no_duplicate(prefix, execution_id, computation, repo) do
    new_id = Journey.Helpers.Random.object_id("CMP")
    node_name_str = Atom.to_string(computation.node_name)
    comp_type_str = Atom.to_string(computation.computation_type)
    now = System.os_time(:second)

    %{num_rows: num_rows} =
      repo.query!(
        """
        INSERT INTO computations (id, execution_id, node_name, computation_type, state, inserted_at, updated_at)
        SELECT $1::varchar, $2::varchar, $3::varchar, $4::varchar, 'not_set', $5::bigint, $5::bigint
        WHERE NOT EXISTS (
          SELECT 1 FROM computations
          WHERE execution_id = $2
            AND node_name = $3
            AND (
              state IN ('not_set', 'computing')
              OR (state = 'success' AND ex_revision_at_start > $6)
            )
        )
        """,
        [new_id, execution_id, node_name_str, comp_type_str, now, computation.ex_revision_at_start]
      )

    if num_rows == 1 do
      Logger.info(
        "#{prefix}: created a new re-computation, #{new_id}.#{node_name_str}. an upstream node has a newer version"
      )

      [new_id]
    else
      []
    end
  end
end
