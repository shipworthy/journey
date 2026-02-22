defmodule Journey.Scheduler.Retry do
  @moduledoc false

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Computation

  require Logger

  defp get_max_upstream_revision(computation, repo) do
    all_values = Journey.Persistence.Values.load_from_db(computation.execution_id, repo)

    graph_node =
      Journey.Scheduler.Helpers.graph_node_from_execution_id(
        computation.execution_id,
        computation.node_name
      )

    upstream_node_names =
      Journey.Node.UpstreamDependencies.Computations.list_all_node_names(graph_node.gated_by)

    all_values
    |> Enum.filter(fn v -> v.node_name in upstream_node_names and v.set_time != nil end)
    |> Enum.map(fn v -> v.ex_revision end)
    |> Enum.reject(fn r -> is_nil(r) end)
    |> Enum.max(fn -> 0 end)
  end

  def maybe_schedule_a_retry(computation, repo) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}]"
    Logger.info("#{prefix}: starting")

    node_name_as_string = computation.node_name |> Atom.to_string()

    current_max_upstream_revision = get_max_upstream_revision(computation, repo)

    number_of_recent_tries =
      from(
        c in Computation,
        where:
          c.execution_id == ^computation.execution_id and
            c.node_name == ^node_name_as_string and
            c.ex_revision_at_start >= ^current_max_upstream_revision,
        select: count(c.id)
      )
      |> repo.one()

    graph_node = Journey.Scheduler.Helpers.graph_node_from_execution_id(computation.execution_id, computation.node_name)

    if number_of_recent_tries < graph_node.max_retries do
      new_computation =
        %Execution.Computation{
          execution_id: computation.execution_id,
          node_name: node_name_as_string,
          computation_type: computation.computation_type,
          state: :not_set
        }
        |> repo.insert!()

      Logger.info("#{prefix}: creating a new computation, #{new_computation.id}")
    else
      Logger.info(
        "#{prefix}: reached max retries (#{number_of_recent_tries} attempts >= max #{graph_node.max_retries}), not rescheduling"
      )
    end

    computation
  end
end
