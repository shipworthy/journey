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
      iex> {:ok, execution} = Journey.set_value(execution, :name, "Alice")
      iex> Journey.Tools.what_am_i_waiting_for(execution.id, :greeting) |> IO.puts()
      ✅ :name | &is_set/1 | rev 1
      🛑 :title | &is_set/1
      :ok
      iex> {:ok, execution} = Journey.set_value(execution, :title, "Dr.")
      iex> {:ok, _greeting_value} = Journey.get_value(execution, :greeting, wait_new: true)
      iex> Journey.Tools.what_am_i_waiting_for(execution.id, :greeting) |> IO.puts()
      ✅ :name | &is_set/1 | rev 1
      ✅ :title | &is_set/1 | rev 2
      :ok
  """
  def what_am_i_waiting_for(execution_id, computation_node_name)
      when is_binary(execution_id) and is_atom(computation_node_name) do
    execution = Journey.load(execution_id)
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    computation_node = Enum.find(execution.computations, fn c -> c.node_name == computation_node_name end)

    outstanding_computation(graph, execution.values, %{
      node_name: computation_node.node_name,
      state: computation_node.state,
      computation_type: computation_node.computation_type
    })
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
      iex> execution = Journey.set_value(execution, :value, 5)
      iex> {:ok, _result} = Journey.get_value(execution, :double, wait_new: true)
      iex> Journey.Tools.computation_state(execution.id, :double)
      :success
  """
  def computation_state(execution_id, node_name)
      when is_binary(execution_id) and is_atom(node_name) do
    execution = Journey.load(execution_id)
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

  @doc false
  def outstanding_computations(execution_id) when is_binary(execution_id) do
    execution = Journey.load(execution_id)
    graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)

    all_candidates_for_computation =
      from(c in Computation,
        where:
          c.execution_id == ^execution_id and
            c.state == ^:not_set and
            c.computation_type in [^:compute, ^:schedule_once, ^:schedule_recurring],
        lock: "FOR UPDATE"
      )
      |> Journey.Repo.all()
      |> Journey.Executions.convert_values_to_atoms(:node_name)

    all_value_nodes =
      from(v in Value, where: v.execution_id == ^execution_id)
      |> Journey.Repo.all()
      |> Enum.map(fn %{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)
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

  @doc false
  def increment_revision(execution_id, value_node_name) when is_binary(execution_id) and is_atom(value_node_name) do
    value_node_name_str = Atom.to_string(value_node_name)

    Journey.Repo.transaction(fn repo ->
      [value_node] =
        from(v in Value,
          where: v.execution_id == ^execution_id and v.node_name == ^value_node_name_str,
          lock: "FOR UPDATE"
        )
        |> repo.all()
        |> Enum.map(fn %{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)

      new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution_id, repo)

      value_node
      |> Ecto.Changeset.change(%{
        ex_revision: new_revision
      })
      |> repo.update!()
    end)

    Journey.load(execution_id)
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

      iex> Journey.Tools.summarize("EXEC07B2H0H7J1LTAE0VJDAL")
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

  Use `summarize_to_text/1` to format this data as human-readable text.
  """
  def summarize(execution_id) when is_binary(execution_id) do
    execution =
      execution_id
      |> Journey.load(include_archived: true)

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
  Formats execution summary data as human-readable text.

  Takes structured data from `summarize/1` and converts it to the same detailed
  text format that was previously provided by the original `summarize/1` function.

  ## Example

      iex> summary_data = Journey.Tools.summarize("EXEC07B2H0H7J1LTAE0VJDAL")
      iex> Journey.Tools.summarize_to_text(summary_data) |> IO.puts()
      Execution summary:
      - ID: 'EXEC07B2H0H7J1LTAE0VJDAL'
      - Graph: 'g1' | 'v1'
      - Archived at: not archived
      - Created at: 2025-08-14 17:23:16Z UTC | 49348 seconds ago
      - Last updated at: 2025-08-14 17:23:30Z UTC | 49334 seconds ago
      - Duration: 14 seconds
      - Revision: 7
      - # of Values: 5 (set) / 5 (total)
      - # of Computations: 2
      ...
      :ok

  ## Parameters
  - `summary_data` - The structured data map returned by `summarize/1`

  ## Returns
  A formatted string with the complete execution state summary.

  Useful for debugging blocked executions and understanding computation dependencies.
  """
  def summarize_to_text(summary_data) when is_map(summary_data) do
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
      Enum.map_join(set_values, "\n", fn %{
                                           node_type: node_type,
                                           node_name: node_name,
                                           set_time: set_time,
                                           node_value: node_value,
                                           ex_revision: ex_revision
                                         } ->
        verb = if node_type == :input, do: "set", else: "computed"

        "  - #{node_name}: '#{inspect(node_value)}' | #{inspect(node_type)}\n" <>
          "    #{verb} at #{DateTime.from_unix!(set_time)} | rev: #{ex_revision}\n"
      end) <>
      """
      \n
      - Not set:
      """ <>
      Enum.map_join(not_set_values, "\n", fn %{node_type: node_type, node_name: node_name} ->
        "  - #{node_name}: <unk> | #{inspect(node_type)}"
      end) <>
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
         with_header? \\ false
       ) do
    readiness =
      Journey.Node.UpstreamDependencies.Computations.evaluate_computation_for_readiness(
        values,
        graph
        |> Graph.find_node_by_name(node_name)
        |> Map.get(:gated_by)
      )

    indent = if(with_header?, do: "       ", else: "")

    if(with_header?,
      do: "  - #{node_name}: #{computation_state_to_text(state)} | #{inspect(computation_type)}\n",
      else: ""
    ) <>
      Enum.map_join(readiness.conditions_met, "", fn %{upstream_node: v, f_condition: f} ->
        "#{indent}✅ #{inspect(v.node_name)} | #{f_name(f)} | rev #{v.ex_revision}\n"
      end) <>
      Enum.map_join(readiness.conditions_not_met, "\n", fn %{upstream_node: v, f_condition: f} ->
        "#{indent}🛑 #{inspect(v.node_name)} | #{f_name(f)}"
      end)
  end
end
