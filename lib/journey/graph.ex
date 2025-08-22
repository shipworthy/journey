defmodule Journey.Graph do
  @moduledoc false

  import Journey.Node, only: [input: 1]

  defstruct [:name, :version, :nodes, :f_on_save]
  @type t :: %__MODULE__{name: String.t(), nodes: list, f_on_save: function() | nil}

  def new(name, version, nodes, opts \\ [])
      when is_binary(name) and is_binary(version) and is_list(nodes) do
    opts_schema = [
      f_on_save: [
        is: {:fun, 3},
        required: false,
        doc:
          "Graph-wide callback invoked after any node computation succeeds. Receives (execution_id, node_name, result)."
      ]
    ]

    KeywordValidator.validate!(opts, opts_schema)

    %__MODULE__{
      name: name,
      version: version,
      nodes: [input(:execution_id), input(:last_updated_at)] ++ nodes,
      f_on_save: Keyword.get(opts, :f_on_save)
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
