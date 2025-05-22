defmodule JourneyMermaidConverter do
  @moduledoc """
  Converts Journey graphs to Mermaid graph definitions.
  """

  def compose_mermaid(graph) when is_struct(graph, Journey.Graph) do
    nodes = graph.nodes

    # Build the mermaid definition
    [
      "graph TD",
      build_legend(),
      "",
      build_node_definitions(nodes),
      "",
      build_connections(nodes),
      "",
      build_styling_with_nodes(nodes)
    ]
    |> List.flatten()
    |> Enum.join("\n")
  end

  defp build_legend do
    [
      "    %% Legend",
      "    subgraph Legend[\"📖 Legend\"]",
      "        LegendInput[\"Input Node<br/>User-provided data\"]",
      "        LegendCompute[\"Compute Node<br/>(function_name)<br/>Business logic\"]",
      "        LegendSchedule[\"Schedule Node<br/>(function_name)<br/>schedule_once node<br/>Time-based triggers\"]",
      "        LegendMutate[\"Mutate Node<br/>(function_name)<br/>mutates: target_node<br/>Value transformation\"]",
      "    end",
      ""
    ]
  end

  defp build_node_definitions(nodes) do
    Enum.map(nodes, &build_node_definition/1)
  end

  # Handle Journey.Graph.Input nodes
  defp build_node_definition(%Journey.Graph.Input{name: name}) do
    "    #{sanitize_name(name)}[#{name}]"
  end

  # Handle Journey.Graph.Step nodes
  defp build_node_definition(%Journey.Graph.Step{name: name, type: type, f_compute: f_compute, mutates: mutates}) do
    function_name = extract_function_name(f_compute)

    cond do
      # Check if it's a mutate node (has mutates field set)
      mutates != nil ->
        "    #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})<br/>mutates: #{mutates}\"]"

      # Otherwise check the type
      type == :compute ->
        "    #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})\"]"

      type == :schedule_once ->
        "    #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})<br/>schedule_once node\"]"

      type == :schedule_recurring ->
        "    #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})<br/>schedule_recurring node\"]"

      true ->
        "    #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})\"]"
    end
  end

  # Fallback for any other node types
  defp build_node_definition(%{name: name}) do
    "    #{sanitize_name(name)}[#{name}]"
  end

  defp build_connections(nodes) do
    Enum.flat_map(nodes, &build_node_connections/1)
  end

  # Handle Journey.Graph.Input nodes (no dependencies)
  defp build_node_connections(%Journey.Graph.Input{}) do
    []
  end

  # Handle Journey.Graph.Step nodes with dependencies
  defp build_node_connections(%Journey.Graph.Step{name: name, gated_by: gated_by}) do
    extract_connections(name, gated_by)
  end

  # Fallback for other node types
  defp build_node_connections(_node) do
    []
  end

  defp extract_connections(name, gated_by) when is_list(gated_by) do
    # Simple list of dependencies
    Enum.map(gated_by, fn dep ->
      "    #{sanitize_name(dep)} --> #{sanitize_name(name)}"
    end)
  end

  defp extract_connections(name, gated_by) when is_atom(gated_by) do
    # Single dependency
    ["    #{sanitize_name(gated_by)} --> #{sanitize_name(name)}"]
  end

  defp extract_connections(name, gated_by) do
    # Handle any unblocked_when conditions or other complex structures
    deps = list_all_node_names(gated_by)

    Enum.map(deps, fn dep ->
      "    #{sanitize_name(dep)} --> #{sanitize_name(name)}"
    end)
  end

  # Use the same logic as Journey.Node.UpstreamDependencies.Computations.list_all_node_names/1
  defp list_all_node_names(node_names) when is_list(node_names) do
    node_names
  end

  defp list_all_node_names({:not, {node_name, _condition}}) when is_atom(node_name) do
    [node_name]
  end

  defp list_all_node_names({operation, conditions}) when operation in [:and, :or] and is_list(conditions) do
    conditions
    |> Enum.flat_map(&list_all_node_names/1)
  end

  defp list_all_node_names({upstream_node_name, _f_condition}) when is_atom(upstream_node_name) do
    [upstream_node_name]
  end

  defp list_all_node_names(_), do: []

  defp extract_function_name(f) when is_function(f) do
    # Try to extract function name from function info
    try do
      case Function.info(f) do
        [module: module, name: name, arity: _arity, env: _, type: :external] ->
          module_name = module |> Module.split() |> List.last()
          "#{module_name}.#{name}"

        _ ->
          "anonymous fn"
      end
    rescue
      _ -> "anonymous fn"
    end
  end

  defp extract_function_name({module, function}) do
    module_name = module |> Module.split() |> List.last()
    "#{module_name}.#{function}"
  end

  defp extract_function_name({module, function, _arity}) do
    module_name = module |> Module.split() |> List.last()
    "#{module_name}.#{function}"
  end

  defp extract_function_name(_) do
    "anonymous fn"
  end

  defp sanitize_name(name) when is_atom(name) do
    name
    |> Atom.to_string()
    |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
  end

  defp build_styling_with_nodes(nodes) do
    # Categorize nodes by type
    {input_nodes, compute_nodes, schedule_nodes, mutate_nodes} =
      categorize_nodes(nodes)

    [
      "    %% Styling",
      "    classDef inputNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000000",
      "    classDef computeNode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000000",
      "    classDef scheduleNode fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000000",
      "    classDef mutateNode fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px,color:#000000",
      "",
      "    %% Apply styles to legend nodes",
      "    class LegendInput inputNode",
      "    class LegendCompute computeNode",
      "    class LegendSchedule scheduleNode",
      "    class LegendMutate mutateNode",
      "",
      "    %% Apply styles to actual nodes",
      build_class_assignment("inputNode", input_nodes),
      build_class_assignment("computeNode", compute_nodes),
      build_class_assignment("scheduleNode", schedule_nodes),
      build_class_assignment("mutateNode", mutate_nodes)
    ]
    |> List.flatten()
    |> Enum.reject(&(&1 == []))
  end

  defp categorize_nodes(nodes) do
    Enum.reduce(nodes, {[], [], [], []}, fn node, {input, compute, schedule, mutate} ->
      sanitized_name = sanitize_name(node.name)

      case node do
        %Journey.Graph.Input{} ->
          {[sanitized_name | input], compute, schedule, mutate}

        %Journey.Graph.Step{mutates: mutates} when mutates != nil ->
          {input, compute, schedule, [sanitized_name | mutate]}

        %Journey.Graph.Step{type: :compute} ->
          {input, [sanitized_name | compute], schedule, mutate}

        %Journey.Graph.Step{type: :schedule_once} ->
          {input, compute, [sanitized_name | schedule], mutate}

        %Journey.Graph.Step{type: :schedule_recurring} ->
          {input, compute, [sanitized_name | schedule], mutate}

        _ ->
          {input, [sanitized_name | compute], schedule, mutate}
      end
    end)
  end

  defp build_class_assignment(_class_name, []), do: []

  defp build_class_assignment(class_name, node_names) do
    node_list = Enum.join(node_names, ",")
    "    class #{node_list} #{class_name}"
  end
end

# Usage example:
# graph = Journey.Examples.CreditCardApplication.graph()
# mermaid_definition = JourneyMermaidConverter.compose_mermaid(graph)
# IO.puts(mermaid_definition)
