defmodule Journey.Graph.Catalog do
  use Agent

  def start_link(_) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def register(%{name: name} = graph) when is_binary(name) and is_struct(graph, Journey.Graph) do
    :ok = Agent.update(__MODULE__, fn state -> state |> Map.put(name, graph) end)
    graph
  end

  def fetch!(graph_name) when is_binary(graph_name) do
    Agent.get(__MODULE__, fn state -> state |> Map.get(graph_name) end)
  end
end
