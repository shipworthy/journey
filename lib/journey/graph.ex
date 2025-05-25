defmodule Journey.Graph do
  @moduledoc false

  import Journey.Node, only: [input: 1]

  defstruct [:name, :version, :nodes]
  @type t :: %__MODULE__{name: String.t(), nodes: list}

  def new(name, version, nodes)
      when is_binary(name) and is_binary(version) and is_list(nodes) do
    %__MODULE__{
      name: name,
      version: version,
      nodes: [input(:execution_id), input(:last_updated_at)] ++ nodes
    }
  end

  def find_node_by_name(nil, node_name) when is_atom(node_name) do
    nil
  end

  def find_node_by_name(graph, node_name) when is_struct(graph, Journey.Graph) and is_atom(node_name) do
    graph.nodes
    |> Enum.find(fn n -> n.name == node_name end)
  end
end
