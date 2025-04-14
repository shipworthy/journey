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
end
