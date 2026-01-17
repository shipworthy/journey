defmodule Journey.Scheduler.Invalidate do
  @moduledoc false

  import Ecto.Query
  require Logger

  alias Journey.Graph
  alias Journey.Node.UpstreamDependencies
  alias Journey.Persistence.Schema.Execution

  @doc """
  Iteratively clears all discardable computations - computed values whose dependencies
  are no longer met. Continues until no more values can be discarded.
  """
  def ensure_all_discardable_cleared(execution_id, graph) do
    prefix = "[#{execution_id}]"
    Logger.debug("#{prefix}: starting invalidation check")

    if graph == nil do
      Logger.error("#{prefix}: graph is nil")
      nil
    else
      do_iterative_clearing(execution_id, graph, prefix)
    end
  end

  defp do_iterative_clearing(execution_id, graph, prefix) do
    {:ok, cleared_count} =
      Journey.Scheduler.Helpers.transaction_with_deadlock_retry(
        fn repo ->
          all_values = Journey.Persistence.Values.load_from_db(execution_id, repo)
          clear_discardable_computations_in_transaction(execution_id, all_values, graph, repo, prefix)
        end,
        prefix
      )
      |> case do
        {:ok, count} ->
          {:ok, count}

        {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} ->
          Logger.warning(
            "#{prefix}: Failed after retries due to deadlock, " <>
              "stopping invalidation check (will retry on next computation)"
          )

          {:ok, 0}

        {:error, other} ->
          Logger.error("#{prefix}: Transaction failed with error: #{inspect(other)}")
          raise other
      end

    if cleared_count > 0 do
      Logger.info("#{prefix}: cleared #{cleared_count} discardable computations, checking for more...")
      # Recursively clear until nothing left
      do_iterative_clearing(execution_id, graph, prefix)
    else
      Logger.debug("#{prefix}: no discardable computations found, invalidation complete")
      nil
    end
  end

  defp clear_discardable_computations_in_transaction(execution_id, all_values, graph, repo, prefix) do
    # Find :compute node values which might need to be cleared. Other types (eg :historian) preserve their state.
    set_computed_values =
      all_values
      |> Enum.filter(fn v ->
        v.set_time != nil and
          match?(%{type: :compute}, Graph.find_node_by_name(graph, v.node_name))
      end)

    Logger.debug("#{prefix}: checking #{length(set_computed_values)} computed values for discardability")

    # Check each for discardability and clear if needed
    cleared_count =
      set_computed_values
      |> Enum.reduce(0, fn value_node, acc ->
        process_value_node(value_node, acc, execution_id, all_values, graph, repo, prefix)
      end)

    cleared_count
  end

  defp process_value_node(value_node, acc, execution_id, all_values, graph, repo, prefix) do
    graph_node = Graph.find_node_by_name(graph, value_node.node_name)

    cond do
      graph_node == nil ->
        Logger.warning("#{prefix}: graph node not found for #{value_node.node_name}")
        acc

      should_clear?(all_values, graph_node) ->
        Logger.info("#{prefix}: clearing discardable computation: #{value_node.node_name}")
        clear_discardable_computation(execution_id, value_node.node_name, graph_node, repo, prefix)
        acc + 1

      true ->
        acc
    end
  end

  defp should_clear?(all_values, graph_node) do
    not UpstreamDependencies.Computations.unblocked?(all_values, graph_node.gated_by, :invalidation)
  end

  defp clear_discardable_computation(execution_id, node_name, graph_node, repo, prefix) do
    new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution_id, repo)
    now_seconds = System.system_time(:second)

    # Clear the value
    {1, _} =
      from(v in Execution.Value,
        where: v.execution_id == ^execution_id and v.node_name == ^Atom.to_string(node_name)
      )
      |> repo.update_all(
        set: [
          node_value: nil,
          set_time: nil,
          ex_revision: new_revision,
          updated_at: now_seconds
        ]
      )

    # Create new computation for future execution (following recompute pattern)
    new_computation =
      %Execution.Computation{
        execution_id: execution_id,
        node_name: Atom.to_string(node_name),
        computation_type: graph_node.type,
        state: :not_set
      }
      |> repo.insert!()

    Logger.debug("#{prefix}: created new computation #{new_computation.id} for #{node_name}")

    # Update last_updated_at
    from(v in Execution.Value,
      where: v.execution_id == ^execution_id and v.node_name == "last_updated_at"
    )
    |> repo.update_all(
      set: [
        ex_revision: new_revision,
        node_value: now_seconds,
        updated_at: now_seconds,
        set_time: now_seconds
      ]
    )
  end
end
