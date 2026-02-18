defmodule Journey.Executions do
  @moduledoc false
  alias Journey.Persistence.Schema.Execution
  import Ecto.Query

  require Logger

  # Namespace for PostgreSQL advisory locks used in singleton execution creation
  @singleton_lock_namespace 67_890

  # Delegate value operations to Journey.Executions.Values
  defdelegate set_value(execution_id_or_execution, node_name, value, metadata \\ nil),
    to: Journey.Executions.Values

  defdelegate set_values(execution, values_map, metadata \\ nil),
    to: Journey.Executions.Values

  defdelegate unset_value(execution, node_name),
    to: Journey.Executions.Values

  defdelegate unset_values(execution, node_names),
    to: Journey.Executions.Values

  defdelegate get_value(execution, node_name, timeout_ms, opts \\ []),
    to: Journey.Executions.Values

  defdelegate get_value_node(execution, node_name, timeout_ms, opts \\ []),
    to: Journey.Executions.Values

  # Delegate query operations to Journey.Executions.Query
  defdelegate list(graph_name, graph_version, sort_by_fields, value_filters, limit, offset, include_archived?),
    to: Journey.Executions.Query

  defdelegate count(graph_name, graph_version, value_filters, include_archived?),
    to: Journey.Executions.Query

  # Delegate graph schema evolution to Journey.Executions.GraphSchemaEvolution
  defdelegate migrate_to_current_graph_if_needed(execution),
    to: Journey.Executions.GraphSchemaEvolution,
    as: :evolve_if_needed

  def create_new(graph_name, graph_version, nodes, graph_hash, execution_id_prefix) do
    Logger.info("graph '#{graph_name}' (version '#{graph_version}'), id prefix [#{execution_id_prefix}]")

    {:ok, execution} =
      Journey.Repo.transaction(fn repo ->
        execution =
          %Execution{
            id: Journey.Helpers.Random.object_id(execution_id_prefix),
            graph_name: graph_name,
            graph_version: graph_version,
            graph_hash: graph_hash,
            revision: 0
          }
          |> repo.insert!()

        now = System.system_time(:second)

        # Create a value record for every graph node, regardless of the graph node's type.
        _values =
          nodes
          |> Enum.map(fn graph_node ->
            # credo:disable-for-lines:10 Credo.Check.Refactor.Nesting
            {set_time, node_value} =
              case graph_node.name do
                :execution_id -> {now, execution.id}
                :last_updated_at -> {now, now}
                _ -> {nil, nil}
              end

            %Execution.Value{
              execution: execution,
              node_name: Atom.to_string(graph_node.name),
              node_type: graph_node.type,
              ex_revision: execution.revision,
              set_time: set_time,
              node_value: node_value
            }
            |> repo.insert!()
          end)

        # Create computations for computable nodes.
        _computations =
          nodes
          |> Enum.filter(fn %{type: type} -> type in Execution.ComputationType.values() end)
          |> Enum.map(fn computation ->
            %Execution.Computation{
              execution: execution,
              node_name: Atom.to_string(computation.name),
              computation_type: computation.type,
              # ex_revision_at_start: execution.revision,
              state: :not_set
            }
            |> repo.insert!()
          end)

        load(execution.id, true, false)
      end)

    execution
  end

  @doc """
  Returns an existing execution for the graph, or creates a new one if none exists.
  Uses PostgreSQL advisory locks to prevent race conditions.
  """
  def get_or_create(graph) do
    # Fast path: check without lock first
    case find_singleton_execution(graph.name) do
      %Execution{} = execution ->
        execution

      nil ->
        create_singleton_with_lock(graph)
    end
  end

  defp find_singleton_execution(graph_name) do
    from(e in Execution,
      where: e.graph_name == ^graph_name and is_nil(e.archived_at),
      order_by: [asc: e.inserted_at],
      limit: 1,
      preload: [:values, :computations]
    )
    |> Journey.Repo.one()
    |> convert_node_names_to_atoms()
  end

  defp create_singleton_with_lock(graph) do
    lock_key = :erlang.phash2({:singleton, graph.name})

    {:ok, execution} =
      Journey.Repo.transaction(fn repo ->
        # Acquire advisory lock scoped to this graph name
        repo.query!("SELECT pg_advisory_xact_lock($1, $2)", [@singleton_lock_namespace, lock_key])

        # Re-check after acquiring lock (another process may have created it)
        case find_singleton_execution(graph.name) do
          %Execution{} = existing ->
            existing

          nil ->
            create_new(
              graph.name,
              graph.version,
              graph.nodes,
              graph.hash,
              graph.execution_id_prefix
            )
        end
      end)

    execution
  end

  defp q_execution(execution_id, include_archived?) when include_archived? == false do
    from(
      e in Execution,
      where: e.id == ^execution_id and is_nil(e.archived_at)
    )
  end

  defp q_execution(execution_id, include_archived?) when include_archived? == true do
    from(
      e in Execution,
      where: e.id == ^execution_id
    )
  end

  def load(execution_id, preload?, include_archived?, computation_states \\ nil)
      when is_binary(execution_id) and is_boolean(preload?) and is_boolean(include_archived?) do
    execution =
      if preload? do
        q_execution(execution_id, include_archived?)
        |> Journey.Repo.one()
        |> preload_execution(computation_states)
        |> convert_node_names_to_atoms()
      else
        q_execution(execution_id, include_archived?)
        |> Journey.Repo.one()
      end

    Journey.Executions.GraphSchemaEvolution.evolve_if_needed(execution, computation_states)
  end

  @doc false
  def preload_execution(nil, _computation_states), do: nil

  def preload_execution(execution, nil) do
    Journey.Repo.preload(execution, [:values, :computations])
  end

  def preload_execution(execution, computation_states) do
    computations_query = from(c in Execution.Computation, where: c.state in ^computation_states)
    Journey.Repo.preload(execution, [:values, computations: computations_query])
  end

  def values(execution) do
    execution.values
    |> Enum.map(fn value ->
      node_status =
        if is_nil(value.set_time) do
          :not_set
        else
          {:set, value.node_value}
        end

      {value.node_name, node_status}
    end)
    |> Enum.into(%{})
  end

  def archive_execution(execution_id) do
    prefix = "[#{execution_id}]"
    Logger.info("#{prefix}: archiving execution")

    {:ok, archived_at_time} =
      Journey.Repo.transaction(fn repo ->
        current_execution =
          from(e in Execution, where: e.id == ^execution_id)
          |> repo.one!()

        if current_execution.archived_at != nil do
          Logger.info("#{prefix}: execution already archived (#{current_execution.archived_at})")
          current_execution.archived_at
        else
          now = System.system_time(:second)
          Logger.info("#{prefix}: setting archived_at to #{now}")
          Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution_id, repo)

          from(e in Execution, where: e.id == ^execution_id)
          |> Journey.Repo.update_all(set: [archived_at: now, updated_at: now])

          now
        end
      end)

    archived_at_time
  end

  def unarchive_execution(execution_id) do
    prefix = "[#{execution_id}]"
    Logger.info("#{prefix}: unarchiving execution")

    {:ok, :ok} =
      Journey.Repo.transaction(fn repo ->
        current_execution =
          from(e in Execution, where: e.id == ^execution_id)
          |> repo.one!()

        if current_execution.archived_at == nil do
          Logger.info("#{prefix}: execution not archived, nothing to do")
          :ok
        else
          Logger.info("#{prefix}: setting archived_at property to nil")
          now = System.system_time(:second)
          Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution_id, repo)

          from(e in Execution, where: e.id == ^execution_id)
          |> Journey.Repo.update_all(set: [archived_at: nil, updated_at: now])

          :ok
        end
      end)

    :ok
  end

  @doc false
  def find_value_by_name(execution, node_name) when is_atom(node_name) do
    execution.values |> Enum.find(fn value -> value.node_name == node_name end)
  end

  @doc false
  def convert_node_names_to_atoms(nil), do: nil

  def convert_node_names_to_atoms(%Execution{} = execution) do
    computations =
      convert_values_to_atoms(execution.computations, :node_name)
      |> Enum.map(fn
        %{computed_with: nil} = c ->
          c

        c ->
          Map.update!(c, :computed_with, &convert_all_keys_to_atoms/1)
      end)

    %Execution{
      execution
      | values: convert_values_to_atoms(execution.values, :node_name),
        computations: computations
    }
  end

  @doc false
  def convert_values_to_atoms(collection_of_maps, key) do
    collection_of_maps
    |> Enum.map(fn
      nil ->
        nil

      map ->
        Map.update!(map, key, &String.to_atom/1)
    end)
  end

  @doc false
  def convert_all_keys_to_atoms(nil), do: nil

  def convert_all_keys_to_atoms(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
    |> Enum.into(%{})
  end

  @doc false
  def computation_db_to_atoms(nil), do: nil

  def computation_db_to_atoms(computation)
      when is_struct(computation, Journey.Persistence.Schema.Execution.Computation) do
    computation
    |> Map.update!(:node_name, fn n -> String.to_atom(n) end)
    |> Map.update!(:computed_with, &convert_all_keys_to_atoms/1)
  end

  @doc false
  def find_computations_by_node_name(execution, node_name) when is_atom(node_name) do
    execution.computations |> Enum.filter(fn c -> c.node_name == node_name end)
  end

  def history(execution_id) do
    history_of_computations =
      execution_id
      |> Journey.load()
      |> Map.get(:computations)
      |> Enum.filter(fn %{state: s} -> s == :success end)
      |> Enum.sort_by(fn %{ex_revision_at_completion: ex_revision_at_completion} -> ex_revision_at_completion end)
      |> Enum.map(fn cn ->
        %{
          computation_or_value: :computation,
          node_name: cn.node_name,
          node_type: cn.computation_type,
          revision: cn.ex_revision_at_completion,
          revision_at_start: cn.ex_revision_at_start
        }
      end)

    history_of_values =
      execution_id
      |> Journey.load()
      |> Map.get(:values)
      |> Enum.filter(fn %{set_time: st} -> st != nil end)
      |> Enum.sort_by(fn %{ex_revision: r} -> r end, :asc)
      |> Enum.map(fn vn ->
        %{
          computation_or_value: :value,
          node_name: vn.node_name,
          node_type: vn.node_type,
          revision: vn.ex_revision,
          value: vn.node_value,
          revision_at_start: vn.ex_revision
        }
      end)

    history = history_of_computations ++ history_of_values

    history
    |> Enum.sort_by(
      fn %{
           revision: revision,
           revision_at_start: revision_at_start,
           computation_or_value: computation_or_value,
           node_name: node_name
         } ->
        {revision, revision_at_start, computation_or_value, node_name}
      end,
      :asc
    )
    |> Enum.map(fn h -> Map.delete(h, :revision_at_start) end)
  end
end
