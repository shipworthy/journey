defmodule Journey.Executions.GraphSchemaEvolution do
  @moduledoc false
  # Handles live graph schema evolution for executions when graph definitions change.
  #
  # When a graph definition is updated (e.g., new nodes added), existing executions
  # in the database were created with the old graph schema. This module detects
  # schema drift by comparing graph hashes and automatically adds missing nodes to
  # executions, enabling additive graph schema evolution without manual database
  # migrations.
  #
  # Graph schema evolution is triggered transparently on every execution load.

  alias Journey.Persistence.Schema.Execution
  import Ecto.Query

  # Namespace for PostgreSQL advisory locks used in graph schema evolution
  @evolution_lock_namespace 12_345

  @doc """
  Evolves an execution's graph schema to match the current graph definition if needed.

  Compares the execution's graph_hash with the current graph definition.
  If they differ, adds any missing nodes to the execution.

  Uses PostgreSQL advisory locks to prevent concurrent evolution of the
  same execution.
  """
  def evolve_if_needed(nil), do: nil

  def evolve_if_needed(execution) do
    case Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version) do
      nil ->
        # Graph not found in catalog, proceed with original execution
        execution

      current_graph ->
        evolve_to_graph_if_needed(execution, current_graph)
    end
  end

  defp evolve_to_graph_if_needed(execution, graph) do
    if execution.graph_hash == graph.hash do
      # Hashes match, no evolution needed
      execution
    else
      # Execution has no hash (old execution) or hashes differ, evolve
      evolve_to_graph(execution, graph)
    end
  end

  defp evolve_to_graph(execution, graph) do
    {:ok, evolved_execution} =
      Journey.Repo.transaction(fn repo ->
        # Acquire advisory lock to prevent concurrent evolution of the same execution
        lock_key = :erlang.phash2(execution.id)
        repo.query!("SELECT pg_advisory_xact_lock($1, $2)", [@evolution_lock_namespace, lock_key])

        # Reload execution after acquiring lock to get the latest state
        current_execution = load_execution_for_evolution(repo, execution.id)

        # Check if evolution is still needed (another process might have just done it)
        if current_execution.graph_hash == graph.hash do
          # Already evolved by another process
          current_execution
        else
          # Proceed with evolution
          perform_evolution(repo, execution.id, current_execution, graph)
        end
      end)

    evolved_execution
  end

  defp perform_evolution(repo, execution_id, current_execution, graph) do
    # Find missing nodes
    missing_node_names = find_missing_nodes(current_execution, graph)

    # Add missing nodes if any
    if MapSet.size(missing_node_names) > 0 do
      add_missing_nodes(repo, execution_id, graph, missing_node_names)
    end

    # Update execution's graph_hash
    from(e in Execution, where: e.id == ^execution_id)
    |> repo.update_all(set: [graph_hash: graph.hash])

    # Reload and return the evolved execution
    Journey.Executions.load(execution_id, true, false)
  end

  defp load_execution_for_evolution(repo, execution_id) do
    from(e in Execution,
      where: e.id == ^execution_id,
      preload: [:values, :computations]
    )
    |> repo.one!()
    |> Journey.Executions.convert_node_names_to_atoms()
  end

  defp find_missing_nodes(current_execution, graph) do
    # Get current node names from execution
    existing_node_names =
      current_execution.values
      |> Enum.map(& &1.node_name)
      |> MapSet.new()

    # Get expected node names from graph
    expected_node_names =
      graph.nodes
      |> Enum.map(& &1.name)
      |> MapSet.new()

    # Find missing nodes (as atoms)
    MapSet.difference(expected_node_names, existing_node_names)
  end

  defp add_missing_nodes(repo, execution_id, graph, missing_node_names) do
    graph.nodes
    |> Enum.filter(fn node ->
      MapSet.member?(missing_node_names, node.name)
    end)
    |> Enum.each(fn graph_node ->
      add_node_records(repo, execution_id, graph_node)
    end)
  end

  defp add_node_records(repo, execution_id, graph_node) do
    # Create value record with ex_revision: 0 for new nodes
    %Execution.Value{
      execution_id: execution_id,
      node_name: Atom.to_string(graph_node.name),
      node_type: graph_node.type,
      ex_revision: 0,
      set_time: nil,
      node_value: nil
    }
    |> repo.insert!()

    # If it's a compute node, also create a computation record
    if graph_node.type in Execution.ComputationType.values() do
      %Execution.Computation{
        execution_id: execution_id,
        node_name: Atom.to_string(graph_node.name),
        computation_type: graph_node.type,
        state: :not_set
      }
      |> repo.insert!()
    end
  end
end
