defmodule Journey.Tools do
  @moduledoc """
  This module contains utility functions for the Journey library.
  """

  require Logger

  import Ecto.Query

  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph
  alias Journey.Node.UpstreamDependencies

  def computation_state(execution_id, computation_node_name)
      when is_binary(execution_id) and is_atom(computation_node_name) do
    execution = Journey.load(execution_id)
    graph = Journey.Graph.Catalog.fetch!(execution.graph_name)

    gated_by =
      graph
      |> Graph.find_node_by_name(computation_node_name)
      |> Map.get(:gated_by)

    all_value_nodes = Journey.Execution.Values.load_from_db(execution.id, Journey.Repo)

    computation_prerequisites =
      UpstreamDependencies.Computations.evaluate_computation_for_readiness(all_value_nodes, gated_by)

    Journey.Scheduler.Introspection.readiness_state(
      computation_prerequisites.ready?,
      computation_prerequisites.conditions_met,
      computation_prerequisites.conditions_not_met,
      computation_node_name
    )
  end

  @doc """
  Returns details on a computations.
  """
  def outstanding_computations(execution_id) when is_binary(execution_id) do
    execution = Journey.load(execution_id)
    graph = Journey.Graph.Catalog.fetch!(execution.graph_name)

    all_candidates_for_computation =
      from(c in Computation,
        where:
          c.execution_id == ^execution_id and
            c.state == ^:not_set and
            c.computation_type in [^:compute, ^:schedule_once, ^:schedule_recurring],
        lock: "FOR UPDATE SKIP LOCKED"
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
          lock: "FOR UPDATE SKIP LOCKED"
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
end
