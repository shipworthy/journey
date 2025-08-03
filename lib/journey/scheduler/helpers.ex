defmodule Journey.Scheduler.Helpers do
  @moduledoc false

  import Ecto.Query

  require Logger

  alias Journey.Schema.Execution
  import Journey.Helpers.Log

  def graph_from_execution_id(execution_id) do
    execution =
      execution_id
      |> Journey.Executions.load(false, true)

    Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
  end

  def graph_node_from_execution_id(execution_id, node_name) do
    execution_id
    |> graph_from_execution_id()
    |> Journey.Graph.find_node_by_name(node_name)
  end

  def increment_execution_revision_in_transaction(execution_id, repo) do
    if !repo.in_transaction?() do
      raise "#{mf()} must be called inside a transaction"
    end

    {1, [new_revision]} =
      from(e in Execution,
        update: [
          inc: [revision: 1],
          set: [updated_at: ^System.os_time(:second)]
        ],
        where: e.id == ^execution_id,
        select: e.revision
      )
      |> repo.update_all([])

    new_revision
  end
end
