defmodule Journey.Graph do
  @moduledoc false

  import Journey.Node, only: [input: 1]

  defstruct [:name, :version, :nodes, :f_on_save, :hash]

  @type t :: %__MODULE__{
          name: String.t(),
          version: String.t(),
          nodes: list,
          f_on_save: function() | nil,
          hash: String.t()
        }

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

    all_nodes = [input(:execution_id), input(:last_updated_at)] ++ nodes

    %__MODULE__{
      name: name,
      version: version,
      nodes: all_nodes,
      f_on_save: Keyword.get(opts, :f_on_save),
      hash: compute_hash(all_nodes)
    }
  end

  def find_node_by_name(nil, node_name) when is_atom(node_name) do
    nil
  end

  def find_node_by_name(graph, node_name) when is_struct(graph, Journey.Graph) and is_atom(node_name) do
    graph.nodes
    |> Enum.find(fn n -> n.name == node_name end)
  end

  defp compute_hash(nodes) do
    nodes
    |> Enum.sort_by(& &1.name)
    |> Enum.map(&extract_hash_components/1)
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16()
  end

  defp extract_hash_components(node) do
    base_components = {node.name, node.type}

    case node do
      %{gated_by: gated_by} ->
        {base_components, gated_by}

      _ ->
        base_components
    end
  end
end
