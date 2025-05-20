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

  def find_node_by_name(nil, node_name) when is_atom(node_name) do
    nil
  end

  def find_node_by_name(graph, node_name) when is_struct(graph, Journey.Graph) and is_atom(node_name) do
    graph.nodes
    |> Enum.find(fn n -> n.name == node_name end)
  end

  def validate(graph) do
    graph
    |> ensure_no_duplicate_node_names()
    |> validate_dependencies()
  end

  defp validate_dependencies(graph) do
    all_node_names = Enum.map(graph.nodes, & &1.name)

    graph.nodes
    |> Enum.each(fn node -> validate_node(node, all_node_names) end)

    graph
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

  defp validate_node(%Journey.Graph.Step{} = step, all_node_names) do
    # TODO: implement
    # MapSet.new(step.upstream_nodes)
    all_upstream_node_names = []

    unknown_deps = MapSet.difference(MapSet.new(all_upstream_node_names), MapSet.new(all_node_names))

    if Enum.any?(unknown_deps) do
      raise "Unknown upstream nodes in input node '#{inspect(step.name)}': #{Enum.join(unknown_deps, ", ")}"
    end

    if not is_nil(step.mutates) and step.mutates not in all_node_names do
      raise "Mutation node '#{inspect(step.name)}' mutates an unknown node '#{inspect(step.mutates)}'"
    end

    if not is_nil(step.mutates) and step.mutates == step.name do
      raise "Mutation node '#{inspect(step.name)}' attempts to mutate itself"
    end

    step
  end

  defp validate_node(%Journey.Graph.Input{} = input, _all_node_names) do
    input
  end
end
