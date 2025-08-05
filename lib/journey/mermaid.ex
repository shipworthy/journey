defmodule JourneyMermaidConverter do
  @moduledoc false

  def compose_mermaid(graph, opts \\ []) when is_struct(graph, Journey.Graph) do
    show_legend = Keyword.get(opts, :legend, true)
    nodes = graph.nodes

    legend_section = if show_legend, do: [build_legend(), ""], else: []

    # Build the mermaid definition
    [
      "graph TD",
      graph_section(nodes, graph.name, graph.version),
      legend_section,
      generated_at(),
      build_styling_with_nodes(nodes, show_legend)
    ]
    |> List.flatten()
    |> Enum.join("\n")
  end

  defp graph_section(nodes, graph_name, graph_version) do
    [
      "    %% Graph",
      "    subgraph Graph[\"🧩 '#{graph_name}', version #{graph_version}\"]",
      build_node_definitions(nodes),
      "",
      build_connections(nodes),
      "    end",
      ""
    ]
  end

  # Add a version without legend for cleaner graphs
  def compose_mermaid_no_legend(graph) when is_struct(graph, Journey.Graph) do
    compose_mermaid(graph, legend: false)
  end

  defp build_legend do
    [
      "    %% Legend",
      "    subgraph Legend[\"📖 Legend\"]",
      "        LegendInput[\"Input Node<br/>User-provided data\"]",
      "        LegendCompute[\"Compute Node<br/>Self-computing value\"]",
      "        LegendSchedule[\"Schedule Node<br/>Scheduled trigger\"]",
      "        LegendMutate[\"Mutate Node<br/>Mutates the value of another node\"]",
      "    end"
    ]
  end

  defp build_node_definitions(nodes) do
    Enum.map(nodes, &build_node_definition/1)
  end

  # Handle Journey.Graph.Input nodes
  defp build_node_definition(%Journey.Graph.Input{name: name}) do
    "        #{sanitize_name(name)}[#{name}]"
  end

  defp build_node_definition(%Journey.Graph.Step{name: name, type: type, f_compute: f_compute, mutates: mutates}) do
    function_name = extract_function_name(f_compute)

    cond do
      mutates != nil ->
        "        #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})<br/>mutates: #{mutates}\"]"

      type == :compute ->
        "        #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})\"]"

      type == :schedule_once ->
        "        #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})<br/>schedule_once node\"]"

      type == :schedule_recurring ->
        "        #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})<br/>schedule_recurring node\"]"

      true ->
        "        #{sanitize_name(name)}[\"#{name}<br/>(#{function_name})\"]"
    end
  end

  # Fallback for any other node types
  defp build_node_definition(%{name: name}) do
    "        #{sanitize_name(name)}[#{name}]"
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

  defp extract_connections(name, gated_by) when is_atom(gated_by) do
    # Single dependency
    ["        #{sanitize_name(gated_by)} --> #{sanitize_name(name)}"]
  end

  defp extract_connections(name, gated_by) do
    node_names_and_functions = Journey.Node.UpstreamDependencies.Computations.upstream_nodes_and_functions(gated_by)

    Enum.map(node_names_and_functions, fn {node_name, function} ->
      caption =
        function
        |> extract_condition_function_name()
        |> case do
          "" -> ""
          function_name -> "|#{function_name}|"
        end

      "        #{sanitize_name(node_name)} --> #{caption} #{sanitize_name(name)}"
    end)
  end

  defp extract_function_name(f) when is_function(f) do
    case Function.info(f) do
      [module: _module, name: name, arity: _arity, env: _, type: :external] ->
        "#{name}"

      _ ->
        "anonymous fn"
    end
  end

  defp extract_condition_function_name(f) when is_function(f) do
    f
    |> Function.info()
    |> case do
      [module: _module, name: name, arity: _arity, env: _, type: :external] ->
        if name == :provided? do
          ""
        else
          "#{name}"
        end

      _ ->
        ""
    end
  end

  defp sanitize_name(name) when is_atom(name) do
    name
    |> Atom.to_string()
    |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
  end

  defp build_styling_with_nodes(nodes, show_legend) do
    # Categorize nodes by type
    {input_nodes, compute_nodes, schedule_nodes, mutate_nodes} =
      categorize_nodes(nodes)

    legend_styles =
      if show_legend do
        [
          "    %% Apply styles to legend nodes",
          "    class LegendInput inputNode",
          "    class LegendCompute computeNode",
          "    class LegendSchedule scheduleNode",
          "    class LegendMutate mutateNode",
          ""
        ]
      else
        []
      end

    [
      "    %% Styling",
      "    classDef inputNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000000",
      "    classDef computeNode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000000",
      "    classDef scheduleNode fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000000",
      "    classDef mutateNode fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px,color:#000000",
      "",
      legend_styles,
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

  def generated_at() do
    """
        %% Caption
        caption[/"Generated at #{DateTime.utc_now(:second)} UTC"/]
    """
  end
end

# Usage example:
# graph = Journey.Examples.CreditCardApplication.graph()
# mermaid_definition = JourneyMermaidConverter.compose_mermaid(graph)
# IO.puts(mermaid_definition)
