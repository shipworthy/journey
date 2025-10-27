defmodule Journey.Graph.Validations do
  @moduledoc false

  def validate(graph) do
    graph
    |> ensure_no_duplicate_node_names()
    |> ensure_no_cycles()
    |> validate_dependencies()
  end

  def ensure_known_node_name(execution, node_name) do
    # Fetch the graph from catalog to get all node names
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
    all_node_names = graph.nodes |> Enum.map(& &1.name)

    if node_name in all_node_names do
      :ok
    else
      raise "'#{inspect(node_name)}' is not a known node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid node names: #{inspect(Enum.sort(all_node_names))}."
    end
  end

  def ensure_known_input_node_name(graph, node_name)
      when is_struct(graph, Journey.Graph) and is_atom(node_name) do
    all_input_node_names =
      graph.nodes
      |> Enum.filter(fn n -> n.type == :input end)
      |> Enum.map(& &1.name)

    if node_name in all_input_node_names do
      :ok
    else
      raise "'#{inspect(node_name)}' is not a valid input node in graph '#{graph.name}'.'#{graph.version}'. Valid input node names: #{inspect(Enum.sort(all_input_node_names))}."
    end
  end

  def ensure_known_input_node_name(execution, node_name)
      when is_struct(execution, Journey.Persistence.Schema.Execution) and is_atom(node_name) do
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

  def ensure_known_node_names(graph_name, graph_version, node_names)
      when is_binary(graph_name) and is_binary(graph_version) and is_list(node_names) do
    graph = Journey.Graph.Catalog.fetch(graph_name, graph_version)

    if graph do
      validate_node_names_against_graph(graph, node_names)
    else
      raise ArgumentError,
            "Graph '#{graph_name}' version '#{graph_version}' not found. " <>
              "Graphs must be created with Journey.new_graph/3 or registered in config: " <>
              "config :journey, :graphs, [function_that_returns_graph]"
    end
  end

  defp validate_node_names_against_graph(graph, node_names) do
    all_node_names = graph.nodes |> Enum.map(& &1.name)

    Enum.each(node_names, fn node_name ->
      unless node_name in all_node_names do
        raise ArgumentError,
              "Sort field :#{node_name} does not exist in graph '#{graph.name}' version '#{graph.version}'. " <>
                "Available nodes: #{inspect(Enum.sort(all_node_names))}"
      end
    end)
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

    if not is_nil(step.mutates) and step.update_revision and MapSet.member?(all_upstream_node_names, step.mutates) do
      raise "Mutation node '#{inspect(step.name)}' with update_revision: true creates a cycle by mutating '#{inspect(step.mutates)}' which is in its upstream dependencies"
    end

    step
  end

  defp validate_node(%Journey.Graph.Input{} = input, _all_node_names) do
    input
  end

  defp ensure_no_cycles(graph) do
    adjacency_list = build_adjacency_list(graph)
    detect_cycles(adjacency_list, graph.name)
    graph
  end

  defp build_adjacency_list(graph) do
    graph.nodes
    |> Enum.reduce(%{}, fn node, acc ->
      dependencies = get_node_dependencies(node)
      Map.put(acc, node.name, dependencies)
    end)
  end

  defp get_node_dependencies(%Journey.Graph.Input{}), do: []

  defp get_node_dependencies(%Journey.Graph.Step{} = step) do
    step.gated_by
    |> Journey.Node.UpstreamDependencies.Computations.list_all_node_names()
  end

  defp detect_cycles(adjacency_list, graph_name) do
    all_nodes = Map.keys(adjacency_list) |> Enum.sort()
    visited = MapSet.new()
    rec_stack = MapSet.new()

    Enum.reduce(all_nodes, visited, fn node, acc_visited ->
      if MapSet.member?(acc_visited, node) do
        acc_visited
      else
        check_node_for_cycle(node, adjacency_list, acc_visited, rec_stack, [], graph_name)
      end
    end)
  end

  defp check_node_for_cycle(node, adjacency_list, visited, rec_stack, path, graph_name) do
    case dfs_cycle_check(node, adjacency_list, visited, rec_stack, path) do
      {:cycle, cycle_path} ->
        raise_cycle_error(cycle_path, graph_name)

      {:ok, new_visited} ->
        new_visited
    end
  end

  defp dfs_cycle_check(node, adjacency_list, visited, rec_stack, path) do
    visited = MapSet.put(visited, node)
    rec_stack = MapSet.put(rec_stack, node)
    path = [node | path]

    dependencies = Map.get(adjacency_list, node, []) |> Enum.sort()

    result = check_dependencies_for_cycles(dependencies, adjacency_list, visited, rec_stack, path)

    case result do
      {:cycle, cycle_path} -> {:cycle, cycle_path}
      {:ok, final_visited} -> {:ok, MapSet.delete(final_visited, node)}
    end
  end

  defp check_dependencies_for_cycles(dependencies, adjacency_list, visited, rec_stack, path) do
    Enum.reduce_while(dependencies, {:ok, visited}, fn dep, {:ok, acc_visited} ->
      process_dependency(dep, adjacency_list, acc_visited, rec_stack, path)
    end)
  end

  defp process_dependency(dep, adjacency_list, acc_visited, rec_stack, path) do
    cond do
      MapSet.member?(rec_stack, dep) ->
        # Found a back edge - this is a cycle
        cycle_start_index = Enum.find_index(path, fn n -> n == dep end)
        cycle_path = Enum.take(path, cycle_start_index + 1) |> Enum.reverse()
        # Add the back edge to show the complete cycle
        complete_cycle_path = cycle_path ++ [dep]
        {:halt, {:cycle, complete_cycle_path}}

      not MapSet.member?(acc_visited, dep) ->
        case dfs_cycle_check(dep, adjacency_list, acc_visited, rec_stack, path) do
          {:cycle, cycle_path} -> {:halt, {:cycle, cycle_path}}
          {:ok, new_visited} -> {:cont, {:ok, new_visited}}
        end

      true ->
        {:cont, {:ok, acc_visited}}
    end
  end

  defp raise_cycle_error(cycle_path, graph_name) do
    cycle_description = Enum.map_join(cycle_path, " → ", &inspect/1)
    raise "Circular dependency detected in graph '#{graph_name}': #{cycle_description}"
  end
end
