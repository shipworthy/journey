defmodule Journey.Executions do
  @moduledoc false

  @doc """
  Returns the configured backend module for executions storage.
  """
  def backend do
    Application.get_env(:journey, :store, :postgres)
    |> case do
      :postgres -> Journey.ExecutionsPostgres
      :inmemory -> Journey.ExecutionsInMemory
      other -> raise "Unknown storage backend: #{inspect(other)}. Supported backends: :postgres, :inmemory"
    end
  end

  # Delegate all public functions to the configured backend

  def create_new(graph_name, graph_version, nodes, graph_hash) do
    backend().create_new(graph_name, graph_version, nodes, graph_hash)
  end

  def load(execution_id, preload?, include_archived?) do
    backend().load(execution_id, preload?, include_archived?)
  end

  def values(execution) do
    backend().values(execution)
  end

  def unset_value(execution, node_name) do
    backend().unset_value(execution, node_name)
  end

  def unset_values(execution, node_names) do
    backend().unset_values(execution, node_names)
  end

  def set_value(execution_or_id, node_name, value) do
    backend().set_value(execution_or_id, node_name, value)
  end

  def set_values(execution, values_map) do
    backend().set_values(execution, values_map)
  end

  def get_value(execution, node_name, timeout_ms, opts \\ []) do
    backend().get_value(execution, node_name, timeout_ms, opts)
  end

  def list(graph_name, graph_version, sort_by_fields, value_filters, limit, offset, include_archived?) do
    backend().list(graph_name, graph_version, sort_by_fields, value_filters, limit, offset, include_archived?)
  end

  def archive_execution(execution_id) do
    backend().archive_execution(execution_id)
  end

  def unarchive_execution(execution_id) do
    backend().unarchive_execution(execution_id)
  end

  def find_value_by_name(execution, node_name) do
    backend().find_value_by_name(execution, node_name)
  end

  def convert_values_to_atoms(collection_of_maps, key) do
    backend().convert_values_to_atoms(collection_of_maps, key)
  end

  def convert_all_keys_to_atoms(map) do
    backend().convert_all_keys_to_atoms(map)
  end

  def computation_db_to_atoms(computation) do
    backend().computation_db_to_atoms(computation)
  end

  def find_computations_by_node_name(execution, node_name) do
    backend().find_computations_by_node_name(execution, node_name)
  end

  def history(execution_id) do
    backend().history(execution_id)
  end

  def migrate_to_current_graph_if_needed(execution) do
    backend().migrate_to_current_graph_if_needed(execution)
  end
end
