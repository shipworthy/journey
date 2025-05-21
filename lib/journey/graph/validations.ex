defmodule Journey.Graph.Validations do
  @moduledoc false

  def validate(graph) do
    graph
    |> ensure_no_duplicate_node_names()
    |> validate_dependencies()
  end

  defp validate_dependencies(graph) do
    all_node_names = Enum.map(graph.nodes, & &1.name)
    graph.nodes |> Enum.each(fn node -> validate_node(node, all_node_names) end)
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
    all_upstream_node_names =
      step.gated_by
      |> Journey.Node.UpstreamDependencies.Computations.list_all_node_names()
      |> MapSet.new()

    unknown_deps = MapSet.difference(all_upstream_node_names, MapSet.new(all_node_names))

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
