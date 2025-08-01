defmodule Journey.Graph.Catalog do
  @moduledoc false

  use Agent

  def start_link(_) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def register(%{name: name, version: version} = graph)
      when is_binary(name) and is_binary(version) and is_struct(graph, Journey.Graph) do
    :ok = Agent.update(__MODULE__, fn state -> state |> Map.put({name, version}, graph) end)
    graph
  end

  def fetch(graph_name, graph_version)
      when is_binary(graph_name) and is_binary(graph_version) do
    Agent.get(__MODULE__, fn state -> state |> Map.get({graph_name, graph_version}) end)
  end

  def list(graph_name \\ nil, graph_version \\ nil)

  def list(nil, nil) do
    Agent.get(__MODULE__, fn state ->
      state
      |> Map.values()
    end)
  end

  def list(graph_name, nil) when is_binary(graph_name) do
    Agent.get(__MODULE__, fn state ->
      state
      |> Enum.filter(fn {{name, _version}, _graph} -> name == graph_name end)
      |> Enum.map(fn {_key, graph} -> graph end)
      |> Enum.sort_by(& &1.version, :desc)
    end)
  end

  def list(graph_name, graph_version)
      when is_binary(graph_name) and is_binary(graph_version) do
    case fetch(graph_name, graph_version) do
      nil -> []
      graph -> [graph]
    end
  end

  def list(nil, graph_version) when is_binary(graph_version) do
    raise ArgumentError, "graph_version cannot be specified without graph_name"
  end
end
