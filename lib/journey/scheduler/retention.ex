defmodule Journey.Scheduler.Retention do
  @moduledoc false
  # Cleans up old completed computations based on retention settings.
  # Called after a computation completes successfully, outside the completion transaction.

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution.Computation

  require Logger

  @default_keep_oldest 10

  @doc """
  Deletes excess completed computations for a node if retention is configured.
  Keeps the oldest N and latest M successful computations, deleting everything in between.
  """
  def maybe_cleanup(execution_id, graph_node, graph) do
    case effective_keep_latest(graph_node, graph) do
      :all ->
        :ok

      keep_latest ->
        keep_oldest = graph_node.keep_oldest_completed_computations || @default_keep_oldest
        do_cleanup(execution_id, Atom.to_string(graph_node.name), keep_oldest, keep_latest)
    end
  end

  # Resolution:
  #   node integer → use it (node override)
  #   node :all    → keep everything (explicit node opt-out)
  #   node nil     → inherit from graph
  #   graph integer → use it
  #   graph :all or nil → keep everything (backwards-compatible default)
  defp effective_keep_latest(graph_node, graph) do
    case graph_node.keep_latest_completed_computations do
      n when is_integer(n) ->
        n

      :all ->
        :all

      nil ->
        case graph.keep_latest_completed_computations do
          n when is_integer(n) -> n
          _all_or_nil -> :all
        end
    end
  end

  # Single DELETE with subqueries: remove successful computations that are
  # NOT among the oldest N AND NOT among the latest M.
  # A concurrent completion may cause a mild race condition — worst case
  # we keep a few extra rows, which is harmless.
  defp do_cleanup(execution_id, node_name, keep_oldest, keep_latest) do
    {deleted, _} =
      delete_query(execution_id, node_name, keep_oldest, keep_latest)
      |> Journey.Repo.delete_all()

    if deleted > 0 do
      Logger.debug(
        "[retention] #{execution_id}.#{node_name}: deleted #{deleted} " <>
          "(keeping oldest #{keep_oldest} + latest #{keep_latest})"
      )
    end

    :ok
  rescue
    e ->
      Logger.warning("[retention] #{execution_id}.#{node_name}: cleanup failed: #{inspect(e)}")
      :ok
  end

  defp delete_query(execution_id, node_name, keep_oldest, keep_latest) do
    oldest_ids = success_ids_query(execution_id, node_name, :asc, keep_oldest)
    latest_ids = success_ids_query(execution_id, node_name, :desc, keep_latest)

    from(c in Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^node_name and
          c.state == :success and
          c.id not in subquery(oldest_ids) and
          c.id not in subquery(latest_ids)
    )
  end

  defp success_ids_query(execution_id, node_name, order, limit) do
    from(c in Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^node_name and
          c.state == :success,
      order_by: [{^order, c.ex_revision_at_completion}],
      limit: ^limit,
      select: c.id
    )
  end
end
