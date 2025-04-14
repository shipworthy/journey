defmodule Journey.Graph do
  @moduledoc false

  defstruct [:name, :version, :nodes]
  @type t :: %__MODULE__{name: String.t(), nodes: list}

  def new(name, version, nodes)
      when is_binary(name) and is_binary(version) and is_list(nodes) do
    %__MODULE__{
      name: name,
      version: version,
      nodes: nodes
    }
  end

  def find_node_by_name(graph, node_name) when is_struct(graph, Journey.Graph) and is_atom(node_name) do
    graph.nodes
    |> Enum.find(fn n -> n.name == node_name end)
  end

  def validate(graph) do
    graph
    |> ensure_no_duplicate_node_names()
  end

  defp ensure_no_duplicate_node_names(graph) do
    graph.nodes
    |> Enum.map(& &1.name)
    |> Enum.frequencies()
    |> Enum.filter(fn {_, v} -> v > 1 end)
    |> Enum.each(fn {k, _v} ->
      raise "Duplicate node name in graph definition: #{inspect(k)}"
    end)

    graph
  end
end
