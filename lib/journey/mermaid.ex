defmodule JourneyMermaidConverter do
  @moduledoc false

  def compose_mermaid(graph, opts \\ []) when is_struct(graph, Journey.Graph) do
    show_timestamp = Keyword.get(opts, :timestamp, false)
    nodes = graph.nodes

    timestamp_section = if show_timestamp, do: generated_at(), else: []

    # Build the mermaid definition
    [
      "graph TD",
      graph_section(nodes, graph.name, graph.version),
      timestamp_section,
      build_graph_styling(nodes)
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

  def compose_mermaid_execution(graph, node_statuses, opts \\ [])
      when is_struct(graph, Journey.Graph) and is_map(node_statuses) do
    show_legend = Keyword.get(opts, :legend, false)
    show_timestamp = Keyword.get(opts, :timestamp, false)
    nodes = graph.nodes

    legend_section = if show_legend, do: [build_execution_legend(), ""], else: []
    timestamp_section = if show_timestamp, do: generated_at(), else: []

    [
      "graph TD",
      graph_section_with_status(nodes, graph.name, graph.version, node_statuses),
      legend_section,
      timestamp_section,
      build_execution_styling(nodes, node_statuses)
    ]
    |> List.flatten()
    |> Enum.join("\n")
  end

  defp graph_section_with_status(nodes, graph_name, graph_version, node_statuses) do
    [
      "    %% Graph",
      "    subgraph Graph[\"🧩 '#{graph_name}', version #{graph_version}\"]",
      build_node_definitions_with_status(nodes, node_statuses),
      "",
      build_connections(nodes),
      "    end",
      ""
    ]
  end

  defp build_node_definitions_with_status(nodes, node_statuses) do
    Enum.map(nodes, fn node -> build_node_definition_with_status(node, node_statuses) end)
  end

  defp build_node_definition_with_status(%Journey.Graph.Input{name: name}, node_statuses) do
    emoji = Map.get(node_statuses, name, "")
    "        #{sanitize_name(name)}[\"#{emoji} #{name}\"]"
  end

  defp build_node_definition_with_status(
         %Journey.Graph.Step{name: name, type: type, f_compute: f_compute, mutates: mutates},
         node_statuses
       ) do
    emoji = Map.get(node_statuses, name, "")
    function_name = extract_function_name(f_compute)

    cond do
      mutates != nil ->
        "        #{sanitize_name(name)}[[\"#{emoji} #{name}<br/>(#{function_name})<br/>mutates: #{mutates}\"]]"

      type == :compute ->
        "        #{sanitize_name(name)}[[\"#{emoji} #{name}<br/>(#{function_name})\"]]"

      type in [:schedule_once, :tick_once] ->
        "        #{sanitize_name(name)}[[\"#{emoji} #{name}<br/>(#{function_name})<br/>tick_once node\"]]"

      type in [:schedule_recurring, :tick_recurring] ->
        "        #{sanitize_name(name)}[[\"#{emoji} #{name}<br/>(#{function_name})<br/>tick_recurring node\"]]"

      true ->
        "        #{sanitize_name(name)}[[\"#{emoji} #{name}<br/>(#{function_name})\"]]"
    end
  end

  defp build_node_definition_with_status(%{name: name}, node_statuses) do
    emoji = Map.get(node_statuses, name, "")
    "        #{sanitize_name(name)}[\"#{emoji} #{name}\"]"
  end

  defp build_execution_legend do
    [
      "    %% Legend",
      "    subgraph StatusLegend[\"🏷️ Status\"]",
      "        StatusSuccess[\"✅ Success / Set\"]",
      "        StatusComputing[\"⏳ Computing\"]",
      "        StatusBlocked[\"🚫 Blocked\"]",
      "        StatusNotSet[\"⬜ Not yet set / picked up\"]",
      "        StatusFailed[\"❌ Failed\"]",
      "        StatusAbandoned[\"❓ Abandoned\"]",
      "        StatusCancelled[\"🛑 Cancelled\"]",
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
        "        #{sanitize_name(name)}[[\"#{name}<br/>(#{function_name})<br/>mutates: #{mutates}\"]]"

      type == :compute ->
        "        #{sanitize_name(name)}[[\"#{name}<br/>(#{function_name})\"]]"

      type in [:schedule_once, :tick_once] ->
        "        #{sanitize_name(name)}[[\"#{name}<br/>(#{function_name})<br/>tick_once node\"]]"

      type in [:schedule_recurring, :tick_recurring] ->
        "        #{sanitize_name(name)}[[\"#{name}<br/>(#{function_name})<br/>tick_recurring node\"]]"

      true ->
        "        #{sanitize_name(name)}[[\"#{name}<br/>(#{function_name})\"]]"
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

  defp build_graph_styling(nodes) do
    all_nodes = Enum.map(nodes, fn node -> sanitize_name(node.name) end)

    [
      "    %% Styling",
      "    classDef defaultNode fill:#f8f9fa,stroke:#495057,stroke-width:2px,color:#000000",
      "",
      "    %% Apply styles to nodes",
      build_class_assignment("defaultNode", all_nodes)
    ]
    |> List.flatten()
    |> Enum.reject(&(&1 == []))
  end

  defp build_execution_styling(nodes, node_statuses) do
    {set_nodes, computing_nodes, error_nodes, neutral_nodes} =
      Enum.reduce(nodes, {[], [], [], []}, fn node, {set, computing, error, neutral} ->
        sanitized_name = sanitize_name(node.name)
        emoji = Map.get(node_statuses, node.name, "")

        case emoji do
          "✅" -> {[sanitized_name | set], computing, error, neutral}
          "⏳" -> {set, [sanitized_name | computing], error, neutral}
          e when e in ["❌", "🛑", "❓"] -> {set, computing, [sanitized_name | error], neutral}
          _ -> {set, computing, error, [sanitized_name | neutral]}
        end
      end)

    [
      "    %% Styling",
      "    classDef setNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000000",
      "    classDef computingNode fill:#fff8e1,stroke:#f57f17,stroke-width:2px,color:#000000",
      "    classDef errorNode fill:#f8bbd0,stroke:#b71c1c,stroke-width:2px,color:#000000",
      "    classDef neutralNode fill:#f8f9fa,stroke:#495057,stroke-width:2px,color:#000000",
      "",
      "    %% Apply styles to nodes",
      build_class_assignment("setNode", set_nodes),
      build_class_assignment("computingNode", computing_nodes),
      build_class_assignment("errorNode", error_nodes),
      build_class_assignment("neutralNode", neutral_nodes)
    ]
    |> List.flatten()
    |> Enum.reject(&(&1 == []))
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
