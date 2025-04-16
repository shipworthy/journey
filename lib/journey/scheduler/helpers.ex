defmodule Journey.Scheduler.Helpers do
  @moduledoc false

  require Logger

  def graph_from_execution_id(execution_id) do
    execution_id
    |> Journey.Executions.load(false)
    |> Map.get(:graph_name)
    |> Journey.Graph.Catalog.fetch!()
  end

  def graph_node_from_execution_id(execution_id, node_name) do
    execution_id
    |> graph_from_execution_id()
    |> Journey.Graph.find_node_by_name(node_name)
  end
end
