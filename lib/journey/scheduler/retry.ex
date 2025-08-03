defmodule Journey.Scheduler.Retry do
  @moduledoc false

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Computation

  require Logger
  import Journey.Helpers.Log

  def maybe_schedule_a_retry(computation, repo) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [#{mf()}]"
    Logger.info("#{prefix}: starting")

    node_name_as_string = computation.node_name |> Atom.to_string()

    number_of_tries_so_far =
      from(
        c in Computation,
        where: c.execution_id == ^computation.execution_id and c.node_name == ^node_name_as_string,
        select: count(c.id)
      )
      |> repo.one()

    graph_node = Journey.Scheduler.Helpers.graph_node_from_execution_id(computation.execution_id, computation.node_name)

    if number_of_tries_so_far < graph_node.max_retries do
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
      Logger.info("#{prefix}: reached max retries (#{number_of_tries_so_far}), not rescheduling")
    end

    computation
  end
end
