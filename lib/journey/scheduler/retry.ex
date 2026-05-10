defmodule Journey.Scheduler.Retry do
  @moduledoc false

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Computation

  require Logger

  def maybe_schedule_a_retry(computation, repo) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}]"
    Logger.info("#{prefix}: starting")

    node_name_as_string = computation.node_name |> Atom.to_string()

    all_values = Journey.Persistence.Values.load_from_db(computation.execution_id, repo)

    graph_node =
      Journey.Scheduler.Helpers.graph_node_from_execution_id(
        computation.execution_id,
        computation.node_name
      )

    current_max_upstream_revision = Journey.Scheduler.Helpers.max_upstream_revision(all_values, graph_node)

    # The `ex_revision_at_start >= upstream_rev` filter is the per-attempt boundary for
    # `:compute` and other non-loop retry-eligible types: it scopes counting to attempts
    # since the last upstream change. For `:loop`, the per-iteration `loop_iteration`
    # filter is the boundary; the upstream-rev filter is redundant-but-harmless because
    # loop semantics don't anticipate upstream changes mid-iteration. Removing either
    # filter would regress the corresponding type. The loop-iteration filter is applied
    # only when the type is `:loop` — for non-loop rows `loop_iteration` is nil, and
    # `c.loop_iteration == NULL` evaluates to NULL (never true), which would collapse
    # the count to 0 and silently flip non-loop nodes to infinite retries.
    base_query =
      from(
        c in Computation,
        where:
          c.execution_id == ^computation.execution_id and
            c.node_name == ^node_name_as_string and
            c.ex_revision_at_start >= ^current_max_upstream_revision,
        select: count(c.id)
      )

    scoped_query =
      if computation.computation_type == :loop do
        from(c in base_query, where: c.loop_iteration == ^computation.loop_iteration)
      else
        base_query
      end

    number_of_recent_tries = repo.one(scoped_query)

    if number_of_recent_tries < graph_node.max_retries do
      new_computation =
        %Execution.Computation{
          execution_id: computation.execution_id,
          node_name: node_name_as_string,
          computation_type: computation.computation_type,
          state: :not_set,
          # For :loop nodes, preserve the iteration counter so retries resume the same iteration N
          # rather than restarting from 1. Nil for non-loop computations.
          loop_iteration: computation.loop_iteration
        }
        |> repo.insert!()

      Logger.info("#{prefix}: creating a new computation, #{new_computation.id}")
      {:retry_scheduled, computation}
    else
      Logger.info(
        "#{prefix}: reached max retries (#{number_of_recent_tries} attempts >= max #{graph_node.max_retries}), not rescheduling"
      )

      {:retries_exhausted, computation}
    end
  end
end
