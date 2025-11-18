defmodule Journey.Tools do
  @moduledoc """
  This module contains utility functions for the Journey library.
  """

  require Logger

  import Ecto.Query

  alias Journey.Graph
  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  @doc """
  Shows the status of upstream dependencies for a computation node.

  Lists each dependency with a checkmark (✅) if satisfied or a stop sign (🛑) if not.
  Useful for debugging to see which dependencies are met and which are still blocking.

  ## Parameters
  - `execution_id` - The ID of the execution to analyze
  - `computation_node_name` - The atom name of the computation node to check

  ## Returns
  A string showing the readiness status with checkmarks for met conditions and
  stop signs for unmet conditions.

  ## Example

      iex> import Journey.Node
      iex> graph = Journey.new_graph("what_am_i_waiting_for test graph Elixir.Journey.Tools", "v1.0.0", [
      ...>   input(:name),
      ...>   input(:title),
      ...>   compute(:greeting, [:name, :title], fn %{name: name, title: title} ->
      ...>     {:ok, "Hello, \#{title} \#{name}!"}
      ...>   end)
      ...> ])
      iex> {:ok, execution} = Journey.start_execution(graph)
      iex> Journey.Tools.what_am_i_waiting_for(execution.id, :greeting) |> IO.puts()
      🛑 :name | &is_set/1
      🛑 :title | &is_set/1
      :ok
      iex> {:ok, execution} = Journey.set(execution, :name, "Alice")
      iex> Journey.Tools.what_am_i_waiting_for(execution.id, :greeting) |> IO.puts()
      ✅ :name | &is_set/1 | rev 1
      🛑 :title | &is_set/1
      :ok
      iex> {:ok, execution} = Journey.set(execution, :title, "Dr.")
      iex> {:ok, %{value: _greeting_value}} = Journey.get_value(execution, :greeting, wait: :newer)
      iex> Journey.Tools.what_am_i_waiting_for(execution.id, :greeting) |> IO.puts()
      ✅ :name | &is_set/1 | rev 1
      ✅ :title | &is_set/1 | rev 2
      :ok
  """
  def what_am_i_waiting_for(execution_id, computation_node_name)
      when is_binary(execution_id) and is_atom(computation_node_name) do
    execution = Journey.load(execution_id)
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    gated_by = graph |> Graph.find_node_by_name(computation_node_name) |> Map.get(:gated_by)

    readiness =
      Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
        execution.values,
        gated_by
      )

    # Use flat format for this debugging function
    Enum.map_join(readiness.conditions_met, "", fn condition_map = %{upstream_node: v, f_condition: f} ->
      node_display = format_node_name_with_context(v.node_name, condition_map)
      "✅ #{node_display} | #{f_name(f)} | rev #{v.ex_revision}\n"
    end) <>
      Enum.map_join(readiness.conditions_not_met, "\n", fn condition_map = %{upstream_node: v, f_condition: f} ->
        node_display = format_node_name_with_context(v.node_name, condition_map)
        "🛑 #{node_display} | #{f_name(f)}"
      end)
  end

  @doc """
  Returns the current state of a computation node.

  Returns the state of the most recent computation attempt for the given node.
  If no computation has been attempted yet, returns `:not_set`.
  For input nodes (non-compute nodes), returns `:not_compute_node`.

  ## Parameters
  - `execution_id` - The ID of the execution to check
  - `node_name` - The atom name of the node to check

  ## Returns
  - `:not_set` - No computation has been attempted yet
  - `:computing` - Currently computing
  - `:success` - Computation completed successfully
  - `:failed` - Computation failed
  - `:abandoned` - Computation was abandoned
  - `:cancelled` - Computation was cancelled
  - `:not_compute_node` - The node is an input node, not a computation

  ## Examples

      iex> import Journey.Node
      iex> graph = Journey.new_graph("computation_state doctest graph", "v1.0.0", [
      ...>   input(:value),
      ...>   compute(:double, [:value], fn %{value: v} -> {:ok, v * 2} end)
      ...> ])
      iex> execution = Journey.start_execution(graph)
      iex> Journey.Tools.computation_state(execution.id, :double)
      :not_set
      iex> Journey.Tools.computation_state(execution.id, :value)
      :not_compute_node
      iex> execution = Journey.set(execution, :value, 5)
      iex> {:ok, %{value: _result}} = Journey.get_value(execution, :double, wait: :newer)
      iex> Journey.Tools.computation_state(execution.id, :double)
      :success
  """
  def computation_state(execution_id, node_name)
      when is_binary(execution_id) and is_atom(node_name) do
    case Journey.load(execution_id) do
      nil ->
        raise ArgumentError, "Execution '#{execution_id}' not found"

      execution ->
        graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
        graph_node = Graph.find_node_by_name(graph, node_name)

        case graph_node.type do
          :input ->
            :not_compute_node

          _ ->
            Journey.Executions.find_computations_by_node_name(execution, node_name)
            |> computation_state_impl()
        end
    end
  end

  defp computation_state_impl(computations) when computations == [], do: :not_set

  defp computation_state_impl(computations) when is_list(computations) do
    computations
    |> Enum.max_by(fn c -> {c.ex_revision_at_completion || -1, c.id} end)
    |> Map.get(:state)
  end

  @doc """
  Converts a computation state atom to human-readable text with a visual symbol.

  Returns a formatted string with an appropriate symbol and the state atom
  for each computation state, following the pattern used in other Journey
  text formatting functions.

  ## Parameters
  - `state` - The computation state atom returned by `computation_state/2`

  ## Returns
  A string with symbol and the state atom.

  ## State Representations
  - `:not_set` - "⬜ :not_set (not yet attempted)"
  - `:computing` - "⏳ :computing"
  - `:success` - "✅ :success"
  - `:failed` - "❌ :failed"
  - `:abandoned` - "❓ :abandoned"
  - `:cancelled` - "🛑 :cancelled"
  - `:not_compute_node` - "📝 :not_compute_node"

  ## Examples

      iex> Journey.Tools.computation_state_to_text(:success)
      "✅ :success"

      iex> Journey.Tools.computation_state_to_text(:computing)
      "⏳ :computing"

      iex> Journey.Tools.computation_state_to_text(:not_set)
      "⬜ :not_set (not yet attempted)"
  """
  def computation_state_to_text(state) when is_atom(state) do
    case state do
      :not_set -> "⬜ :not_set (not yet attempted)"
      :computing -> "⏳ :computing"
      :success -> "✅ :success"
      :failed -> "❌ :failed"
      :abandoned -> "❓ :abandoned"
      :cancelled -> "🛑 :cancelled"
      :not_compute_node -> "📝 :not_compute_node"
      other -> "? :#{other}"
    end
  end

  @doc """
  Shows the status and dependencies for a single computation node.

  Provides a focused view of one specific computation node's status and dependencies,
  similar to the computation sections in summarize_as_text/1 but for just one node.

  ## Parameters
  - `execution_id` - The ID of the execution to analyze
  - `node_name` - The atom name of the computation node to check

  ## Returns
  A string showing the node's current status and dependencies.

  For completed computations, shows the result with inputs used:

      :send_follow_up (CMPTA5MDJHVXRMG54150EGX): ✅ :success | :compute | rev 4
      inputs used:
         :user_applied (rev 0)
         :card_mailed (rev 0)

  For outstanding computations, shows the dependency tree:

      :send_weekly_reminder (CMPTA5MDJHVXRMG54150EGX): ⬜ :not_set (not yet attempted) | :compute
           :and
            ├─ 🛑 :subscribe_weekly | &true?/1
            ├─ 🛑 :weekly_reminder_schedule | &provided?/1
            └─ ✅ :email_address | &provided?/1 | rev 2

  For input nodes (non-compute nodes), returns an appropriate message.

  ## Examples

      iex> import Journey.Node
      iex> graph = Journey.new_graph("computation_status_as_text doctest", "v1.0.0", [
      ...>   input(:value),
      ...>   compute(:double, [:value], fn %{value: v} -> {:ok, v * 2} end)
      ...> ])
      iex> execution = Journey.start_execution(graph)
      iex> Journey.Tools.computation_status_as_text(execution.id, :double)
      ":double: ⬜ :not_set (not yet attempted) | :compute\\n       ✅ :value | &is_set/1"

      iex> import Journey.Node
      iex> graph = Journey.new_graph("computation_status_as_text completed doctest", "v1.0.0", [
      ...>   input(:value),
      ...>   compute(:triple, [:value], fn %{value: v} -> {:ok, v * 3} end)
      ...> ])
      iex> execution = Journey.start_execution(graph)
      iex> execution = Journey.set(execution, :value, 5)
      iex> {:ok, %{}} = Journey.get_value(execution, :triple, wait: :newer)
      iex> result = Journey.Tools.computation_status_as_text(execution.id, :triple)
      iex> result =~ ":triple"
      true
      iex> result =~ "✅ :success"
      true
      iex> result =~ "inputs used"
      true
  """
  def computation_status_as_text(execution_id, node_name)
      when is_binary(execution_id) and is_atom(node_name) do
    case Journey.load(execution_id) do
      nil ->
        raise ArgumentError, "Execution '#{execution_id}' not found"

      execution ->
        do_computation_status_as_text(execution, execution_id, node_name)
    end
  end

  defp do_computation_status_as_text(execution, execution_id, node_name) do
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    case Graph.find_node_by_name(graph, node_name) do
      nil ->
        "Node :#{node_name} not found in graph"

      graph_node ->
        case graph_node.type do
          :input ->
            ":#{node_name}: 📝 :not_compute_node (input nodes do not compute)"

          _ ->
            state = computation_state(execution_id, node_name)
            format_single_computation_status(execution, graph, graph_node, state)
        end
    end
  end

  defp format_single_computation_status(execution, graph, graph_node, state) do
    case state do
      state when state in [:success, :failed, :abandoned, :cancelled, :computing] ->
        format_completed_computation_for_node(execution, graph_node, state)

      :not_set ->
        format_outstanding_computation_status(execution, graph, graph_node)
    end
  end

  defp format_completed_computation_for_node(execution, graph_node, state) do
    node_name = graph_node.name
    computations = Journey.Executions.find_computations_by_node_name(execution, node_name)

    if Enum.empty?(computations) do
      ":#{node_name}: #{computation_state_to_text(state)} | #{inspect(graph_node.type)}"
    else
      most_recent_computation =
        computations
        |> Enum.max_by(fn c -> {c.ex_revision_at_completion || -1, c.id} end)

      format_completed_computation_status(most_recent_computation, execution, graph_node)
    end
  end

  defp format_completed_computation_status(
         %{
           id: id,
           node_name: node_name,
           state: state,
           computation_type: computation_type,
           computed_with: computed_with,
           ex_revision_at_completion: ex_revision_at_completion
         },
         execution,
         graph_node
       ) do
    header =
      ":#{node_name} (#{id}): #{computation_state_to_text(state)} | #{inspect(computation_type)} | rev #{ex_revision_at_completion}\n"

    # For failed computations with no computed_with data, show the dependency tree
    if state == :failed and empty_computed_with?(computed_with) do
      header <> format_failed_computation_dependencies(execution, graph_node)
    else
      # For successful computations or failed with computed_with data, show what was used
      header <> format_inputs_used(computed_with)
    end
  end

  defp empty_computed_with?(nil), do: true
  defp empty_computed_with?([]), do: true
  defp empty_computed_with?(inputs) when inputs == %{}, do: true
  defp empty_computed_with?(_), do: false

  defp format_failed_computation_dependencies(execution, graph_node) do
    gated_by = Map.get(graph_node, :gated_by)

    if gated_by do
      readiness =
        Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
          execution.values,
          gated_by
        )

      format_condition_tree(readiness.structure, "    ")
    else
      "inputs used:\n   <none>"
    end
  end

  defp format_inputs_used(computed_with) do
    "inputs used:\n" <>
      if empty_computed_with?(computed_with) do
        "   <none>"
      else
        Enum.map_join(computed_with, "\n", fn {node_name, revision} ->
          "   #{inspect(node_name)} (rev #{revision})"
        end)
      end
  end

  defp format_outstanding_computation_status(execution, _graph, graph_node) do
    node_name = graph_node.name
    gated_by = Map.get(graph_node, :gated_by)

    readiness =
      Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
        execution.values,
        gated_by
      )

    # Find the computation ID for this outstanding computation
    computation_id =
      execution.computations
      |> Enum.find(fn c -> c.node_name == node_name and c.state == :not_set end)
      |> case do
        nil -> "NO_COMPUTATION_ID"
        computation -> computation.id
      end

    header =
      ":#{node_name} (#{computation_id}): #{computation_state_to_text(:not_set)} | #{inspect(graph_node.type)}\n"

    formatted_conditions = format_condition_tree(readiness.structure, "    ")

    header <> formatted_conditions
  end

  @doc false
  def outstanding_computations(execution_id) when is_binary(execution_id) do
    execution = Journey.load(execution_id)
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    all_candidates_for_computation =
      from(c in Computation,
        where:
          c.execution_id == ^execution_id and
            c.state == ^:not_set and
            c.computation_type in [
              ^:compute,
              ^:schedule_once,
              ^:tick_once,
              ^:schedule_recurring,
              ^:tick_recurring
            ],
        lock: "FOR UPDATE"
      )
      |> Journey.Repo.all()
      |> Journey.Executions.convert_values_to_atoms(:node_name)

    all_value_nodes =
      from(v in Value, where: v.execution_id == ^execution_id)
      |> Journey.Repo.all()
      |> Enum.map(fn %Value{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)
      |> Enum.map(fn v -> de_ecto(v) end)

    all_candidates_for_computation
    |> Enum.map(fn computation_candidate -> de_ecto(computation_candidate) end)
    |> Enum.map(fn computation_candidate ->
      Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
        all_value_nodes,
        graph
        |> Graph.find_node_by_name(computation_candidate.node_name)
        |> Map.get(:gated_by)
      )
      |> Map.put(:computation, computation_candidate)
    end)
  end

  defp de_ecto(ecto_struct) do
    ecto_struct
    |> Map.drop([:__meta__, :__struct__, :execution_id, :execution])
  end

  @doc false
  def set_computed_node_value(execution_id, computation_node_name, value)
      when is_binary(execution_id) and is_atom(computation_node_name) do
    execution_id
    |> Journey.load()
    |> Journey.Executions.set_value(computation_node_name, value)
  end

  @doc false
  def advance(execution_id) when is_binary(execution_id) do
    execution_id
    |> Journey.load()
    |> Journey.Scheduler.advance()
  end

  @doc """
  Generates structured data about an execution's current state.

  Returns a map containing:
  - Execution metadata (ID, graph, timestamps, duration, revision, archived status)
  - Values categorized as set/not_set with their details
  - Computations categorized as completed/outstanding with dependency info

  ## Example

      iex> Journey.Tools.summarize_as_data("EXEC07B2H0H7J1LTAE0VJDAL")
      %{
        execution_id: "EXEC07B2H0H7J1LTAE0VJDAL",
        graph_name: "g1",
        graph_version: "v1",
        archived_at: nil,
        created_at: 1723656196,
        updated_at: 1723656210,
        duration_seconds: 14,
        revision: 7,
        values: %{
          set: [...],
          not_set: [...]
        },
        computations: %{
          completed: [...],
          outstanding: [...]
        }
      }

  ## Parameters
  - `execution_id` - The ID of the execution to analyze

  ## Returns
  A structured map with execution state data.

  Use `summarize_as_text/1` to get execution summary as text.
  """
  def summarize_as_data(execution_id) when is_binary(execution_id) do
    execution =
      case Journey.load(execution_id, include_archived: true) do
        nil ->
          raise ArgumentError, "Execution '#{execution_id}' not found"

        exec ->
          exec
      end

    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    if graph == nil do
      raise "Graph '#{execution.graph_name}' not found in catalog."
    end

    set_values = execution.values |> Enum.filter(fn v -> v.set_time != nil end) |> Enum.sort_by(& &1.set_time, :desc)
    not_set_values = execution.values |> Enum.filter(fn v -> v.set_time == nil end)

    computations_completed =
      execution.computations
      |> Enum.filter(fn c -> c.ex_revision_at_completion != nil end)
      |> Enum.sort_by(& &1.ex_revision_at_completion, :desc)

    computations_outstanding =
      execution.computations
      |> Enum.filter(fn c -> c.ex_revision_at_completion == nil end)
      |> Enum.sort_by(& &1.node_name, :desc)

    %{
      execution_id: execution_id,
      graph_name: execution.graph_name,
      graph_version: execution.graph_version,
      archived_at: execution.archived_at,
      created_at: execution.inserted_at,
      updated_at: execution.updated_at,
      duration_seconds: execution.updated_at - execution.inserted_at,
      revision: execution.revision,
      values: %{
        set: set_values,
        not_set: not_set_values
      },
      computations: %{
        completed: computations_completed,
        outstanding: computations_outstanding
      },
      graph: graph
    }
  end

  @doc """
  Introspects an execution's current state with a human-readable text summary.

  This is the primary debugging and introspection tool for Journey executions,
  providing a comprehensive snapshot of values, computations, and dependencies.

  ## Example

      iex> Journey.Tools.introspect("EXEC07B2H0H7J1LTAE0VJDAL") |> IO.puts()
      Execution summary:
      - ID: 'EXEC07B2H0H7J1LTAE0VJDAL'
      - Graph: 'g1' | 'v1'
      ...
      :ok

  ## Parameters
  - `execution_id` - The ID of the execution to analyze

  ## Returns
  A formatted string with the complete execution state summary.

  Use `summarize_as_data/1` to get execution summary as structured data.
  """
  def introspect(execution_id) when is_binary(execution_id) do
    execution_id
    |> summarize_as_data()
    |> convert_summary_data_to_text()
  end

  @doc """
  Generates a human-readable text summary of an execution's current state.

  **This function is deprecated.** Use `introspect/1` instead.

  ## Parameters
  - `execution_id` - The ID of the execution to analyze

  ## Returns
  A formatted string with the complete execution state summary.

  Use `summarize_as_data/1` to get execution summary as data.
  """
  @deprecated "Use introspect/1 instead"
  def summarize_as_text(execution_id) when is_binary(execution_id) do
    introspect(execution_id)
  end

  @doc """
  Generates a human-readable text summary of an execution's current state.

  **This function is deprecated.** Use `introspect/1` instead.

  ## Parameters
  - `execution_id` - The ID of the execution to analyze

  ## Returns
  A formatted string with the complete execution state summary.
  """
  @deprecated "Use introspect/1 instead"
  def summarize(execution_id) when is_binary(execution_id) do
    introspect(execution_id)
  end

  defp convert_summary_data_to_text(summary_data) when is_map(summary_data) do
    %{
      execution_id: execution_id,
      graph_name: graph_name,
      graph_version: graph_version,
      archived_at: archived_at,
      created_at: created_at,
      updated_at: updated_at,
      duration_seconds: duration_seconds,
      revision: revision,
      values: %{set: set_values, not_set: not_set_values},
      computations: %{completed: computations_completed, outstanding: computations_outstanding},
      graph: graph
    } = summary_data

    archived_at_text =
      case archived_at do
        nil -> "not archived"
        _ -> DateTime.from_unix!(archived_at)
      end

    now = System.system_time(:second)

    """
    Execution summary:
    - ID: '#{execution_id}'
    - Graph: '#{graph_name}' | '#{graph_version}'
    - Archived at: #{archived_at_text}
    - Created at: #{DateTime.from_unix!(created_at)} UTC | #{now - created_at} seconds ago
    - Last updated at: #{DateTime.from_unix!(updated_at)} UTC | #{now - updated_at} seconds ago
    - Duration: #{Number.Delimit.number_to_delimited(duration_seconds, precision: 0)} seconds
    - Revision: #{revision}
    - # of Values: #{Enum.count(set_values)} (set) / #{Enum.count(set_values) + Enum.count(not_set_values)} (total)
    - # of Computations: #{Enum.count(computations_completed) + Enum.count(computations_outstanding)}

    Values:
    - Set:
    """ <>
      (set_values
       |> Enum.sort_by(fn %{ex_revision: ex_revision, node_name: node_name} ->
         {-ex_revision, node_name}
       end)
       |> Enum.map_join("\n", fn %{
                                   node_type: node_type,
                                   node_name: node_name,
                                   set_time: set_time,
                                   node_value: node_value,
                                   ex_revision: ex_revision
                                 } ->
         verb = if node_type == :input, do: "set", else: "computed"
         formatted_value = format_node_value(node_name, node_value)

         "  - #{node_name}: '#{formatted_value}' | #{inspect(node_type)}\n" <>
           "    #{verb} at #{DateTime.from_unix!(set_time)} | rev: #{ex_revision}\n"
       end)) <>
      """
      \n
      - Not set:
      """ <>
      (not_set_values
       |> Enum.sort_by(fn %{node_name: node_name} -> node_name end)
       |> Enum.map_join("\n", fn %{node_type: node_type, node_name: node_name} ->
         "  - #{node_name}: <unk> | #{inspect(node_type)}"
       end)) <>
      list_computations(graph, set_values ++ not_set_values, computations_completed, computations_outstanding)
  end

  @doc """
  Generates a Mermaid diagram representation of a Journey graph.

  Converts a graph into Mermaid syntax for visualization. By default returns only
  the flow diagram without legend or timestamp.

  ## Quick Example

  ```elixir
  # Just the flow
  mermaid = Journey.Tools.generate_mermaid_graph(graph)

  # Include legend and timestamp
  mermaid = Journey.Tools.generate_mermaid_graph(graph,
    include_legend: true,
    include_timestamp: true
  )
  ```

  ## Options
  * `:include_legend` - Include node type legend (default: `false`)
  * `:include_timestamp` - Include generation timestamp (default: `false`)
  """
  def generate_mermaid_graph(graph, opts \\ []) do
    opts_schema = [
      include_legend: [is: :boolean],
      include_timestamp: [is: :boolean]
    ]

    KeywordValidator.validate!(opts, opts_schema)

    mermaid_opts =
      opts
      |> Keyword.take([:include_legend, :include_timestamp])
      |> Enum.map(fn
        {:include_legend, value} -> {:legend, value}
        {:include_timestamp, value} -> {:timestamp, value}
      end)

    JourneyMermaidConverter.compose_mermaid(graph, mermaid_opts)
  end

  defp f_name(fun) when is_function(fun) do
    fi =
      fun
      |> :erlang.fun_info()

    "&#{fi[:name]}/#{fi[:arity]}"
  end

  defp format_condition_tree(%{type: :or, children: children}, indent) do
    "#{indent}:or\n" <>
      format_children_with_connectors(children, indent)
  end

  defp format_condition_tree(%{type: :and, children: [single_child]}, indent) do
    format_condition_tree(single_child, indent)
  end

  defp format_condition_tree(%{type: :and, children: children}, indent) do
    "#{indent}:and\n" <>
      format_children_with_connectors(children, indent)
  end

  defp format_condition_tree(%{type: :not, child: %{type: :leaf, met?: met?, condition: condition}}, indent) do
    status = if met?, do: "✅", else: "🛑"
    node_display = format_node_name_with_context(condition.upstream_node.node_name, condition)
    revision_info = if met?, do: " | rev #{condition.upstream_node.ex_revision}", else: ""

    "#{indent}#{status} :not(#{node_display}) | #{f_name(condition.f_condition)}#{revision_info}"
  end

  defp format_condition_tree(%{type: :not, child: child}, indent) do
    "#{indent}:not\n" <> format_condition_tree(child, indent <> " └─ ")
  end

  defp format_condition_tree(%{type: :leaf, met?: met?, condition: condition}, indent) do
    status = if met?, do: "✅", else: "🛑"
    node_display = format_node_name_with_context(condition.upstream_node.node_name, condition)
    revision_info = if met?, do: " | rev #{condition.upstream_node.ex_revision}", else: ""

    "#{indent}#{status} #{node_display} | #{f_name(condition.f_condition)}#{revision_info}"
  end

  defp format_child_with_connector(child, child_indent, continuation_indent) do
    case child.type do
      type when type in [:and, :or] ->
        "#{child_indent}#{type}\n" <> format_children_with_connectors(child.children, continuation_indent)

      :not ->
        case child.child.type do
          :leaf -> format_condition_tree(child, child_indent)
          _ -> "#{child_indent}:not\n" <> format_condition_tree(child.child, continuation_indent <> " └─ ")
        end

      :leaf ->
        format_condition_tree(child, child_indent)
    end
  end

  defp format_children_with_connectors(children, base_indent) do
    children
    |> Enum.with_index()
    |> Enum.map_join("\n", fn {child, index} ->
      is_last = index == length(children) - 1
      connector = if is_last, do: " └─ ", else: " ├─ "
      child_indent = base_indent <> connector
      continuation_indent = base_indent <> if(is_last, do: "    ", else: " │  ")

      format_child_with_connector(child, child_indent, continuation_indent)
    end)
  end

  defp list_computations(graph, values, computations_completed, computations_outstanding) do
    {abandoned, completed_non_abandoned} =
      Enum.split_with(computations_completed, fn comp -> comp.state == :abandoned end)

    """
      \n
    Computations:
    - Completed:
    """ <>
      Enum.map_join(completed_non_abandoned, "\n", &completed_computation/1) <>
      if Enum.empty?(abandoned) do
        ""
      else
        """
        \n
        - Abandoned:
        """ <>
          Enum.map_join(abandoned, "\n", &completed_computation/1)
      end <>
      """
      \n
      - Outstanding:
      """ <>
      Enum.map_join(computations_outstanding, "\n", fn oc -> outstanding_computation(graph, values, oc, true) end)
  end

  defp completed_computation(%{
         id: id,
         node_name: node_name,
         state: state,
         computation_type: computation_type,
         computed_with: computed_with,
         ex_revision_at_completion: ex_revision_at_completion
       }) do
    "  - :#{node_name} (#{id}): #{computation_state_to_text(state)} | #{inspect(computation_type)} | rev #{ex_revision_at_completion}\n" <>
      "    inputs used: \n" <>
      case computed_with do
        nil ->
          "       <none>\n"

        [] ->
          "       <none>\n"

        _ ->
          Enum.map_join(computed_with, "\n", fn
            {node_name, revision} ->
              "       #{inspect(node_name)} (rev #{revision})"
          end)
      end
  end

  defp outstanding_computation(
         graph,
         values,
         %{node_name: node_name, state: state, computation_type: computation_type},
         with_header?
       ) do
    gated_by = graph |> Graph.find_node_by_name(node_name) |> Map.get(:gated_by)

    readiness =
      Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
        values,
        gated_by
      )

    header =
      if with_header? do
        "  - #{node_name}: #{computation_state_to_text(state)} | #{inspect(computation_type)}\n"
      else
        ""
      end

    formatted_conditions = format_condition_tree(readiness.structure, "       ")

    header <> formatted_conditions
  end

  # Helper function to format node values appropriately
  defp format_node_value(node_name, node_value) do
    case node_name do
      :execution_id -> node_value
      :last_updated_at -> node_value
      _ -> inspect(node_value)
    end
  end

  @doc """
  Retries a failed computation.

  This function enables retrying computations that have exhausted their max_retries
  by making their previous attempts "stale" through upstream revision changes, then
  creating a new computation for the scheduler to pick up.

  ## Parameters
  - `execution_id` - The ID of the execution containing the failed computation
  - `computation_node_name` - The atom name of the computation node to retry

  ## Returns
  The updated execution struct

  ## Example
      iex> Journey.Tools.retry_computation("EXEC123", :email_horoscope)
      %Journey.Persistence.Schema.Execution{...}

  ## How It Works
  1. Finds upstream dependencies that are currently satisfied
  3. Creates a new :not_set computation for the scheduler to pick up
  4. Previous failed attempts become "stale" in the retry counting logic
  5. The scheduler can now execute the new computation attempt
  """
  def retry_computation(execution_id, computation_node_name)
      when is_binary(execution_id) and is_atom(computation_node_name) do
    execution = Journey.load(execution_id)
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
    graph_node = Journey.Graph.find_node_by_name(graph, computation_node_name)

    # Create a new computation for the scheduler to pick up
    Journey.Repo.transaction(fn repo ->
      %Journey.Persistence.Schema.Execution.Computation{
        execution_id: execution_id,
        node_name: Atom.to_string(computation_node_name),
        computation_type: graph_node.type,
        state: :not_set
      }
      |> repo.insert!()
    end)

    advance(execution_id)
  end

  # Helper function to format node names with conditional context
  defp format_node_name_with_context(node_name, condition_map) do
    case Map.get(condition_map, :condition_context, :direct) do
      :negated -> "not(#{inspect(node_name)})"
      :direct -> inspect(node_name)
    end
  end
end
