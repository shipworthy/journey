defmodule Journey.Graph.Validations do
  @moduledoc false

  def validate(graph) do
    graph
    |> ensure_no_duplicate_node_names()
    |> validate_dependencies()
  end

  def ensure_known_node_name(execution, node_name) do
    all_node_names = execution.values |> Enum.map(& &1.node_name)

    if node_name in all_node_names do
      :ok
    else
      raise "'#{inspect(node_name)}' is not a known node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid node names: #{inspect(Enum.sort(all_node_names))}."
    end
  end

  def ensure_known_input_node_name(execution, node_name)
      when is_struct(execution, Journey.Schema.Execution) and is_atom(node_name) do
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    all_input_node_names =
      graph.nodes
      |> Enum.filter(fn n -> n.type == :input end)
      |> Enum.map(& &1.name)

    if node_name in all_input_node_names do
      :ok
    else
      raise "'#{inspect(node_name)}' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: #{inspect(Enum.sort(all_input_node_names))}."
    end
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
