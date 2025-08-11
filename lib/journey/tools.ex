defmodule Journey.Tools do
  @moduledoc """
  This module contains utility functions for the Journey library.
  """

  require Logger

  import Ecto.Query

  alias Journey.Graph
  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  def computation_state(execution_id, computation_node_name)
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
  Returns details on a computations.
  """
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

  def set_computed_node_value(execution_id, computation_node_name, value)
      when is_binary(execution_id) and is_atom(computation_node_name) do
    execution_id
    |> Journey.load()
    |> Journey.Executions.set_value(computation_node_name, value)
  end

  def advance(execution_id) when is_binary(execution_id) do
    execution_id
    |> Journey.load()
    |> Journey.Scheduler.advance()
  end

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

    archived_at =
      case execution.archived_at do
        nil -> "not archived"
        _ -> DateTime.from_unix!(execution.archived_at)
      end

    now = System.system_time(:second)

    """
    Execution summary:
    - ID: '#{execution_id}'
    - Graph: '#{execution.graph_name}' | '#{execution.graph_version}'
    - Archived at: #{archived_at}
    - Created at: #{DateTime.from_unix!(execution.inserted_at)} UTC | #{now - execution.inserted_at} seconds ago
    - Last updated at: #{DateTime.from_unix!(execution.updated_at)} UTC | #{now - execution.updated_at} seconds ago
    - Duration: #{Number.Delimit.number_to_delimited(execution.updated_at - execution.inserted_at, precision: 0)} seconds
    - Revision: #{execution.revision}
    - # of Values: #{Enum.count(set_values)} (set) / #{Enum.count(execution.values)} (total)
    - # of Computations: #{Enum.count(execution.computations)}

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
      list_computations(graph, execution.values, computations_completed, computations_outstanding)
  end

  def generate_mermaid_graph(graph) do
    JourneyMermaidConverter.compose_mermaid(graph)
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
    "  - :#{node_name} (#{id}): #{inspect(state)} | #{inspect(computation_type)} | rev #{ex_revision_at_completion}\n" <>
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

    if(with_header?, do: "  - #{node_name}: #{inspect(state)} | #{inspect(computation_type)}\n", else: "") <>
      Enum.map_join(readiness.conditions_met, "", fn %{upstream_node: v, f_condition: f} ->
        "#{indent}✅ #{inspect(v.node_name)} | #{f_name(f)} | rev #{v.ex_revision}\n"
      end) <>
      Enum.map_join(readiness.conditions_not_met, "\n", fn %{upstream_node: v, f_condition: f} ->
        "#{indent}🛑 #{inspect(v.node_name)} | #{f_name(f)}"
      end)
  end
end
