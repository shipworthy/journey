defmodule Journey.Executions do
  @moduledoc false
  alias Journey.Persistence.Schema.Execution
  import Ecto.Query

  require Logger

  # Namespace for PostgreSQL advisory locks used in graph migrations
  @migration_lock_namespace 12_345

  # Namespace for PostgreSQL advisory locks used in singleton execution creation
  @singleton_lock_namespace 67_890

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

  def load(execution_id, preload?, include_archived?)
      when is_binary(execution_id) and is_boolean(preload?) and is_boolean(include_archived?) do
    execution =
      if preload? do
        from(e in q_execution(execution_id, include_archived?),
          where: e.id == ^execution_id,
          preload: [:values, :computations]
        )
        |> Journey.Repo.one()
        |> convert_node_names_to_atoms()
      else
        from(e in q_execution(execution_id, include_archived?), where: e.id == ^execution_id)
        |> Journey.Repo.one()
      end

    migrate_to_current_graph_if_needed(execution)
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

  def unset_value(execution, node_name) do
    prefix = "[#{execution.id}] [#{node_name}]"
    Logger.debug("#{prefix}: unsetting value")

    result =
      Journey.Repo.transaction(fn repo ->
        do_unset_value_in_transaction(execution, node_name, repo, prefix)
      end)

    handle_unset_value_result(result, execution, prefix)
  end

  defp do_unset_value_in_transaction(execution, node_name, repo, prefix) do
    current_value_node =
      from(v in Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
      )
      |> repo.one!()

    if current_value_node.set_time == nil do
      Logger.debug("#{prefix}: no need to update, value unchanged, aborting transaction")
      repo.rollback({:no_change, execution})
    else
      new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution.id, repo)
      perform_unset_updates(execution, node_name, new_revision, repo, prefix)
      Journey.load(execution.id)
    end
  end

  defp perform_unset_updates(execution, node_name, new_revision, repo, prefix) do
    now_seconds = System.system_time(:second)

    update_params = [
      ex_revision: new_revision,
      node_value: nil,
      updated_at: now_seconds,
      set_time: nil
    ]

    {update_count, _} =
      from(v in Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
      )
      |> repo.update_all(set: update_params)

    if update_count == 1 do
      from(v in Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == "last_updated_at"
      )
      |> repo.update_all(
        set: [
          ex_revision: new_revision,
          node_value: now_seconds,
          updated_at: now_seconds,
          set_time: now_seconds
        ]
      )

      Logger.debug("#{prefix}: value unset")
    else
      Logger.error("#{prefix}: value not unset, aborting transaction")
      repo.rollback(execution)
    end
  end

  defp handle_unset_value_result(result, execution, prefix) do
    case result do
      {:ok, updated_execution} ->
        Logger.info("#{prefix}: value unset successfully")
        graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
        Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
        # Reload to get the updated revision after invalidation
        updated_execution = Journey.load(updated_execution.id)
        Journey.Scheduler.advance(updated_execution)

      {:error, {:no_change, original_execution}} ->
        Logger.debug("#{prefix}: value already unset")
        original_execution

      {:error, _} ->
        Logger.error("#{prefix}: value not unset, transaction rolled back")
        Journey.load(execution)
    end
  end

  def unset_values(execution, node_names) when is_list(node_names) do
    prefix = "[#{execution.id}]"
    Logger.debug("#{prefix}: unsetting #{length(node_names)} values: #{inspect(node_names)}")

    # Deduplicate node names to avoid processing the same node multiple times
    unique_node_names = Enum.uniq(node_names)

    # Filter out nodes that are already unset BEFORE starting transaction
    nodes_to_unset = filter_nodes_to_unset(execution, unique_node_names)

    if Enum.empty?(nodes_to_unset) do
      # Complete no-op: nothing to unset, return execution as-is
      Logger.debug("#{prefix}: no values to unset, skipping update")
      execution
    else
      Logger.debug("#{prefix}: #{length(nodes_to_unset)} values to unset, proceeding with transaction")

      # Proceed with transaction only for nodes that need unsetting
      result =
        Journey.Repo.transaction(fn repo ->
          new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution.id, repo)
          now_seconds = System.system_time(:second)

          # Unset all the nodes in a single transaction
          unset_nodes_in_transaction(execution.id, nodes_to_unset, new_revision, now_seconds, repo)

          # Update last_updated_at once
          update_last_updated_at(execution.id, new_revision, now_seconds, repo)

          Journey.load(execution.id)
        end)

      case result do
        {:ok, updated_execution} ->
          Logger.info("#{prefix}: values unset successfully, revision: #{updated_execution.revision}")
          graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
          Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
          # Reload to get the updated revision after invalidation
          updated_execution = Journey.load(updated_execution.id)
          Journey.Scheduler.advance(updated_execution)

        {:error, reason} ->
          Logger.error("#{prefix}: values not unset, transaction rolled back, reason: #{inspect(reason)}")
          Journey.load(execution)
      end
    end
  end

  # Helper function to filter out nodes that are already unset
  defp filter_nodes_to_unset(execution, node_names) do
    current_values =
      from(v in Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name in ^Enum.map(node_names, &Atom.to_string/1),
        select: {v.node_name, v.set_time}
      )
      |> Journey.Repo.all()
      |> Map.new()

    # Only include nodes that are currently set (have a set_time)
    Enum.filter(node_names, fn node_name ->
      node_name_str = Atom.to_string(node_name)
      current_values[node_name_str] != nil
    end)
  end

  # Helper function to unset multiple nodes in a transaction
  defp unset_nodes_in_transaction(execution_id, node_names, new_revision, _now_seconds, repo) do
    node_name_strings = Enum.map(node_names, &Atom.to_string/1)

    # Update all value nodes to unset them
    from(v in Execution.Value,
      where: v.execution_id == ^execution_id and v.node_name in ^node_name_strings
    )
    |> repo.update_all(set: [set_time: nil, node_value: nil, ex_revision: new_revision])
  end

  # Validate that map keys are strings (JSONB requires string keys)
  # Raises ArgumentError if atom keys are found
  defp validate_jsonb_map(nil), do: nil

  defp validate_jsonb_map(value) when is_map(value) do
    atom_keys = value |> Map.keys() |> Enum.filter(&is_atom/1)

    if atom_keys != [] do
      raise ArgumentError,
            "Map keys must be strings for JSONB storage. Found atom keys: #{inspect(atom_keys)}"
    end

    value
  end

  defp validate_jsonb_map(value), do: value

  # credo:disable-for-lines:10 Credo.Check.Refactor.CyclomaticComplexity
  def set_value(execution_id_or_execution, node_name, value, metadata \\ nil)

  def set_value(execution_id, node_name, value, metadata) when is_binary(execution_id) do
    prefix = "[#{execution_id}] [#{node_name}]"
    Logger.debug("#{prefix}: setting value, #{inspect(value)}")

    # Validate that map keys are strings (JSONB requires string keys)
    validate_jsonb_map(value)
    validate_jsonb_map(metadata)

    Journey.Repo.transaction(fn repo ->
      new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution_id, repo)

      current_value_node =
        from(v in Execution.Value,
          where: v.execution_id == ^execution_id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.one!()

      updating_to_the_same_value_and_metadata? =
        current_value_node.set_time != nil and current_value_node.node_value != nil and
          current_value_node.node_value == value and current_value_node.metadata == metadata

      if updating_to_the_same_value_and_metadata? do
        Logger.debug("#{prefix}: no need to update, value unchanged, aborting transaction")
        # Load execution for rollback response
        execution = repo.get!(Execution, execution_id)
        repo.rollback({:no_change, execution})
      else
        now_seconds = System.system_time(:second)

        from(v in Execution.Value,
          where: v.execution_id == ^execution_id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.update_all(
          set: [
            ex_revision: new_revision,
            node_value: value,
            metadata: metadata,
            updated_at: now_seconds,
            set_time: now_seconds
          ]
        )
        # credo:disable-for-lines:10 Credo.Check.Refactor.Nesting
        |> case do
          {1, _} ->
            # Update the "last_updated_at" value.
            from(v in Execution.Value,
              where: v.execution_id == ^execution_id and v.node_name == "last_updated_at"
            )
            |> repo.update_all(
              set: [
                ex_revision: new_revision,
                node_value: now_seconds,
                updated_at: now_seconds,
                set_time: now_seconds
              ]
            )

            Logger.debug("#{prefix}: value updated")
            repo.get!(Execution, execution_id)

          _ ->
            raise "Could not find value node to update."
        end
      end
    end)
    |> case do
      {:ok, updated_execution} ->
        Logger.info("#{prefix}: value set successfully")
        graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
        Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
        # Reload to get the updated revision after invalidation
        updated_execution = Journey.load(updated_execution.id)
        Journey.Scheduler.advance(updated_execution)

      {:error, {:no_change, original_execution}} ->
        Logger.debug("#{prefix}: value not set (updating for the same value), transaction rolled back")
        original_execution

      {:error, _} ->
        Logger.error("#{prefix}: value not set, transaction rolled back")
        {:ok, execution} = Journey.load(execution_id)
        execution
    end
  end

  # credo:disable-for-lines:10 Credo.Check.Refactor.CyclomaticComplexity
  def set_value(execution, node_name, value, metadata) do
    prefix = "[#{execution.id}] [#{node_name}]"
    Logger.debug("#{prefix}: setting value, #{inspect(value)}")

    # Validate that map keys are strings (JSONB requires string keys)
    validate_jsonb_map(value)
    validate_jsonb_map(metadata)

    Journey.Repo.transaction(fn repo ->
      new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution.id, repo)

      current_value_node =
        from(v in Execution.Value,
          where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.one!()

      updating_to_the_same_value_and_metadata? =
        current_value_node.set_time != nil and current_value_node.node_value != nil and
          current_value_node.node_value == value and current_value_node.metadata == metadata

      if updating_to_the_same_value_and_metadata? do
        Logger.debug("#{prefix}: no need to update, value unchanged, aborting transaction")
        repo.rollback({:no_change, execution})
      else
        now_seconds = System.system_time(:second)

        from(v in Execution.Value,
          where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.update_all(
          set: [
            ex_revision: new_revision,
            node_value: value,
            metadata: metadata,
            updated_at: now_seconds,
            set_time: now_seconds
          ]
        )
        # credo:disable-for-lines:10 Credo.Check.Refactor.Nesting
        |> case do
          {1, _} ->
            # Update the "last_updated_at" value.
            from(v in Execution.Value,
              where: v.execution_id == ^execution.id and v.node_name == "last_updated_at"
            )
            |> repo.update_all(
              set: [
                ex_revision: new_revision,
                node_value: now_seconds,
                updated_at: now_seconds,
                set_time: now_seconds
              ]
            )

            Logger.debug("#{prefix}: value updated")

          {0, _} ->
            Logger.error("#{prefix}: value not updated, aborting transaction")
            repo.rollback(execution)
        end

        Journey.load(execution.id)
      end
    end)
    |> case do
      {:ok, updated_execution} ->
        Logger.info("#{prefix}: value set successfully")
        graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
        Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
        # Reload to get the updated revision after invalidation
        updated_execution = Journey.load(updated_execution.id)
        Journey.Scheduler.advance(updated_execution)

      {:error, {:no_change, original_execution}} ->
        Logger.debug("#{prefix}: value not set (updating for the same value), transaction rolled back")
        original_execution

      {:error, _} ->
        Logger.error("#{prefix}: value not set, transaction rolled back")
        Journey.load(execution)
    end
  end

  # credo:disable-for-lines:10 Credo.Check.Refactor.CyclomaticComplexity
  def set_values(execution, values_map, metadata \\ nil)

  def set_values(execution, values_map, metadata) when is_map(values_map) do
    prefix = "[#{execution.id}]"
    Logger.debug("#{prefix}: setting #{map_size(values_map)} values: #{inspect(Map.keys(values_map))}")

    # Validate that map keys are strings (JSONB requires string keys)
    Enum.each(values_map, fn {_key, value} -> validate_jsonb_map(value) end)
    validate_jsonb_map(metadata)

    # Filter out unchanged values BEFORE starting transaction
    changed_values = filter_changed_values(execution, values_map, metadata)

    if map_size(changed_values) == 0 do
      # Complete no-op: nothing changed, return execution as-is
      Logger.debug("#{prefix}: no values changed, skipping update")
      execution
    else
      Logger.debug("#{prefix}: #{map_size(changed_values)} values changed, proceeding with transaction")

      # Proceed with transaction only for changed values
      result =
        Journey.Repo.transaction(fn repo ->
          new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution.id, repo)
          now_seconds = System.system_time(:second)

          # Update only the changed values
          update_changed_values_in_transaction(execution.id, changed_values, new_revision, now_seconds, repo, metadata)

          # Update last_updated_at once
          update_last_updated_at(execution.id, new_revision, now_seconds, repo)

          Journey.load(execution.id)
        end)

      case result do
        {:ok, updated_execution} ->
          Logger.info("#{prefix}: values set successfully, revision: #{updated_execution.revision}")
          graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
          Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
          # Reload to get the updated revision after invalidation
          updated_execution = Journey.load(updated_execution.id)
          Journey.Scheduler.advance(updated_execution)
          updated_execution

        {:error, reason} ->
          Logger.error("#{prefix}: transaction failed: #{inspect(reason)}")
          raise "Failed to set values: #{inspect(reason)}"
      end
    end
  end

  defp filter_changed_values(execution, values_map, metadata) do
    # Create a map of current values and metadata for comparison
    current_state =
      execution.values
      |> Enum.filter(fn v -> Map.has_key?(values_map, v.node_name) end)
      |> Map.new(fn v -> {v.node_name, {v.node_value, v.metadata}} end)

    # Filter to only include values or metadata that are actually changing
    values_map
    |> Enum.filter(fn {node_name, new_value} ->
      case Map.get(current_state, node_name) do
        {current_value, current_metadata} ->
          current_value != new_value or current_metadata != metadata

        _ ->
          true
      end
    end)
    |> Map.new()
  end

  defp update_changed_values_in_transaction(execution_id, changed_values, revision, now_seconds, repo, metadata) do
    Enum.each(changed_values, fn {node_name, value} ->
      update_value_in_transaction(execution_id, node_name, value, revision, now_seconds, repo, metadata)
    end)
  end

  defp update_value_in_transaction(execution_id, node_name, value, revision, now_seconds, repo, metadata) do
    {1, _} =
      from(v in Execution.Value,
        where: v.execution_id == ^execution_id and v.node_name == ^Atom.to_string(node_name)
      )
      |> repo.update_all(
        set: [
          ex_revision: revision,
          node_value: value,
          metadata: metadata,
          updated_at: now_seconds,
          set_time: now_seconds
        ]
      )
  end

  defp update_last_updated_at(execution_id, revision, now_seconds, repo) do
    from(v in Execution.Value,
      where: v.execution_id == ^execution_id and v.node_name == "last_updated_at"
    )
    |> repo.update_all(
      set: [
        ex_revision: revision,
        node_value: now_seconds,
        updated_at: now_seconds,
        set_time: now_seconds
      ]
    )
  end

  def get_value(execution, node_name, timeout_ms, opts \\ []) do
    case get_value_node(execution, node_name, timeout_ms, opts) do
      {:ok, value_node} ->
        {:ok, value_node.node_value}

      error ->
        error
    end
  end

  def get_value_node(execution, node_name, timeout_ms, opts \\ []) do
    prefix = "[#{execution.id}] [#{node_name}]"
    wait_new = Keyword.get(opts, :wait_new, false)
    wait_for_revision = Keyword.get(opts, :wait_for_revision, nil)

    log_get_value_start(prefix, timeout_ms, wait_new, wait_for_revision)

    monotonic_time_deadline = calculate_deadline(timeout_ms)

    load_value_with_options(execution, node_name, monotonic_time_deadline, wait_new, wait_for_revision)
    |> log_get_value_result(prefix)
  end

  defp log_get_value_start(prefix, timeout_ms, wait_new, wait_for_revision) do
    Logger.debug(
      "#{prefix}: starting." <>
        if(timeout_ms != nil, do: " blocking, timeout: #{timeout_ms}", else: "") <>
        if(wait_new, do: " (wait_new: true)", else: "") <>
        if(wait_for_revision, do: " (wait_for_revision: #{wait_for_revision})", else: "")
    )
  end

  defp calculate_deadline(nil), do: nil
  defp calculate_deadline(:infinity), do: :infinity
  defp calculate_deadline(ms), do: System.monotonic_time(:millisecond) + ms

  defp load_value_with_options(execution, node_name, monotonic_time_deadline, _wait_new, wait_for_revision)
       when wait_for_revision != nil do
    # New style: wait for specific revision
    load_value_internal(execution, node_name, monotonic_time_deadline, 0, wait_for_revision)
  end

  defp load_value_with_options(execution, node_name, monotonic_time_deadline, true, _wait_for_revision) do
    # Old style or new style :newer: wait for newer revision than current value
    load_value_wait_new(execution, node_name, monotonic_time_deadline, 0)
  end

  defp load_value_with_options(execution, node_name, monotonic_time_deadline, false, nil) do
    # No waiting
    load_value(execution, node_name, monotonic_time_deadline, 0)
  end

  defp log_get_value_result(result, prefix) do
    case result do
      {:ok, _result} ->
        Logger.debug("#{prefix}: done. success")

      {outcome, result} ->
        Logger.info("#{prefix}: done. outcome: '#{inspect(outcome)}', result: '#{inspect(result)}'")
    end

    result
  end

  defp check_computation_status(execution, node_name) do
    node_name_string = Atom.to_string(node_name)
    graph_node = Journey.Scheduler.Helpers.graph_node_from_execution_id(execution.id, node_name)

    if graph_node.type == :input do
      :not_compute_node
    else
      check_compute_node_status(execution, node_name_string, graph_node)
    end
  end

  defp check_compute_node_status(execution, node_name_string, graph_node) do
    active_computation = find_active_computation(execution.id, node_name_string)

    if active_computation do
      :has_active_computation
    else
      check_if_permanently_failed(execution.id, node_name_string, graph_node.max_retries)
    end
  end

  defp find_active_computation(execution_id, node_name_string) do
    from(c in Execution.Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^node_name_string and
          c.state in [:computing, :not_set],
      limit: 1
    )
    |> Journey.Repo.one()
  end

  defp check_if_permanently_failed(execution_id, node_name_string, max_retries) do
    total_failed_attempts =
      from(c in Execution.Computation,
        where:
          c.execution_id == ^execution_id and
            c.node_name == ^node_name_string and
            c.state == :failed,
        select: count(c.id)
      )
      |> Journey.Repo.one()

    if total_failed_attempts >= max_retries do
      :permanently_failed
    else
      :may_retry_soon
    end
  end

  defp load_value(execution, node_name, monotonic_time_deadline, call_count) do
    load_value_internal(execution, node_name, monotonic_time_deadline, call_count, nil)
  end

  defp load_value_internal(execution, node_name, monotonic_time_deadline, call_count, wait_for_revision) do
    wait_new = wait_for_revision != nil

    prefix =
      "[#{execution.id}][#{node_name}][#{call_count}]" <>
        if(wait_new, do: " wait_new", else: "")

    if wait_new do
      Logger.debug("#{prefix}: waiting for revision > #{wait_for_revision}")
    end

    current_value = load_current_value(execution.id, node_name)

    # Handle the different cases
    process_loaded_value(
      current_value,
      execution,
      node_name,
      monotonic_time_deadline,
      call_count,
      wait_for_revision,
      prefix
    )
  end

  defp process_loaded_value(
         value_node,
         execution,
         node_name,
         monotonic_time_deadline,
         call_count,
         wait_for_revision,
         prefix
       )
       when is_nil(value_node) do
    if monotonic_time_deadline == nil do
      Logger.warning("#{prefix}: value not found.")
      {:error, :not_set}
    else
      # Wait for the Value node to be created or updated
      handle_value_not_ready(
        execution,
        node_name,
        monotonic_time_deadline,
        call_count,
        wait_for_revision,
        nil,
        prefix
      )
    end
  end

  defp process_loaded_value(
         %{set_time: set_time, ex_revision: revision, node_value: _value} = value_node,
         execution,
         node_name,
         monotonic_time_deadline,
         call_count,
         wait_for_revision,
         prefix
       ) do
    cond do
      # For wait_new: check if we have a newer revision
      wait_for_revision != nil and set_time != nil and revision > wait_for_revision ->
        Logger.debug("#{prefix}: found newer revision #{revision}")
        {:ok, value_node}

      # For regular load_value: return if value is set
      wait_for_revision == nil and set_time != nil ->
        Logger.info("#{prefix}: have value, returning.")
        {:ok, value_node}

      # Value not set (or not new enough for wait_new)
      true ->
        handle_value_not_ready(
          execution,
          node_name,
          monotonic_time_deadline,
          call_count,
          wait_for_revision,
          value_node,
          prefix
        )
    end
  end

  defp handle_value_not_ready(
         execution,
         node_name,
         monotonic_time_deadline,
         call_count,
         wait_for_revision,
         current_value,
         prefix
       ) do
    case check_computation_status(execution, node_name) do
      :permanently_failed ->
        Logger.info("#{prefix}: computation permanently failed after max retries.")
        {:error, :computation_failed}

      _ ->
        handle_wait_or_timeout(
          execution,
          node_name,
          monotonic_time_deadline,
          call_count,
          wait_for_revision,
          current_value,
          prefix
        )
    end
  end

  defp handle_wait_or_timeout(
         execution,
         node_name,
         monotonic_time_deadline,
         call_count,
         wait_for_revision,
         current_value,
         prefix
       ) do
    if deadline_exceeded?(monotonic_time_deadline) do
      log_timeout(prefix, wait_for_revision, current_value)
      {:error, :not_set}
    else
      log_waiting(prefix, wait_for_revision, current_value, call_count)
      backoff_sleep(call_count)
      load_value_internal(execution, node_name, monotonic_time_deadline, call_count + 1, wait_for_revision)
    end
  end

  defp log_timeout(prefix, wait_for_revision, current_value) do
    if wait_for_revision do
      current_revision = if current_value, do: current_value.ex_revision, else: "none"

      Logger.info(
        "#{prefix}: timeout reached waiting for revision > #{wait_for_revision} (current: #{current_revision})"
      )
    else
      Logger.info("#{prefix}: timeout exceeded or not specified.")
    end
  end

  defp log_waiting(prefix, wait_for_revision, current_value, call_count) do
    if wait_for_revision do
      current_revision = if current_value, do: current_value.ex_revision, else: "none"
      Logger.debug("#{prefix}: revision still #{current_revision}, waiting, call count: #{call_count}")
    else
      Logger.debug("#{prefix}: value not set, waiting, call count: #{call_count}")
    end
  end

  defp load_value_wait_new(execution, node_name, monotonic_time_deadline, call_count)
       when monotonic_time_deadline == :infinity or is_integer(monotonic_time_deadline) do
    # Get the starting revision from the execution parameter
    starting_value = find_value_by_name(execution, node_name)
    starting_revision = if starting_value, do: starting_value.ex_revision, else: 0

    load_value_internal(execution, node_name, monotonic_time_deadline, call_count, starting_revision)
  end

  defp load_current_value(execution_id, node_name) do
    from(v in Execution.Value,
      where: v.execution_id == ^execution_id and v.node_name == ^Atom.to_string(node_name)
    )
    |> Journey.Repo.one()
  end

  defp deadline_exceeded?(monotonic_time_deadline) when is_nil(monotonic_time_deadline), do: true
  defp deadline_exceeded?(monotonic_time_deadline) when monotonic_time_deadline == :infinity, do: false

  defp deadline_exceeded?(monotonic_time_deadline) when is_integer(monotonic_time_deadline) do
    monotonic_time_deadline < System.monotonic_time(:millisecond)
  end

  def list(graph_name, graph_version, sort_by_fields, value_filters, limit, offset, include_archived?)
      when (is_nil(graph_name) or is_binary(graph_name)) and
             (is_nil(graph_version) or is_binary(graph_version)) and
             is_list(sort_by_fields) and
             is_list(value_filters) and
             is_number(limit) and
             is_number(offset) and
             is_boolean(include_archived?) do
    # Normalize and validate sort fields
    {normalized_fields, value_fields} = prepare_sort_fields(sort_by_fields, graph_name, graph_version)

    # Build and execute query
    from(e in Execution, limit: ^limit, offset: ^offset)
    |> filter_archived(include_archived?)
    |> apply_combined_sorting(normalized_fields, value_fields)
    |> filter_by_graph_name(graph_name)
    |> filter_by_graph_version(graph_version)
    |> add_filters(value_filters)
  end

  def count(graph_name, graph_version, value_filters, include_archived?)
      when (is_nil(graph_name) or is_binary(graph_name)) and
             (is_nil(graph_version) or is_binary(graph_version)) and
             is_list(value_filters) and
             is_boolean(include_archived?) do
    # Build and execute count query (no sorting, limit, or offset needed)
    from(e in Execution)
    |> filter_archived(include_archived?)
    |> filter_by_graph_name(graph_name)
    |> filter_by_graph_version(graph_version)
    |> add_count_filters(value_filters)
  end

  defp prepare_sort_fields(sort_by_fields, graph_name, graph_version) do
    normalized_fields = normalize_sort_fields(sort_by_fields)
    value_fields = extract_value_fields(normalized_fields)

    # Validate value fields exist in the graph (if graph_name is provided)
    if graph_name != nil and graph_version != nil and value_fields != [] do
      field_names = Enum.map(value_fields, fn {field, _direction} -> field end)
      Journey.Graph.Validations.ensure_known_node_names(graph_name, graph_version, field_names)
    end

    {normalized_fields, value_fields}
  end

  defp filter_archived(query, true), do: query
  defp filter_archived(query, false), do: from(e in query, where: is_nil(e.archived_at))

  # Get execution table fields dynamically from schema
  defp execution_fields do
    Journey.Persistence.Schema.Execution.__schema__(:fields)
  end

  defp extract_value_fields(normalized_fields) do
    execution_field_set = MapSet.new(execution_fields())

    normalized_fields
    |> Enum.filter(fn {field, _direction} -> field not in execution_field_set end)
  end

  defp apply_combined_sorting(query, all_fields, _value_fields) when all_fields == [] do
    query
  end

  defp apply_combined_sorting(query, all_fields, value_fields) when value_fields == [] do
    # Only execution fields - simple ORDER BY
    execution_order_by = Enum.map(all_fields, fn {field, direction} -> {direction, field} end)
    from(e in query, order_by: ^execution_order_by)
  end

  defp apply_combined_sorting(query, all_fields, value_fields) do
    # Mixed execution and value fields - need JOINs for value fields
    query_with_joins = add_value_joins(query, value_fields)
    order_by_list = build_order_by_list(all_fields, value_fields)
    from(e in query_with_joins, order_by: ^order_by_list)
  end

  defp add_value_joins(query, value_fields) do
    value_fields
    |> Enum.with_index()
    |> Enum.reduce(query, fn {{node_name, _direction}, index}, acc_query ->
      alias_name = String.to_atom("v#{index}")

      from(e in acc_query,
        left_join: v in Journey.Persistence.Schema.Execution.Value,
        as: ^alias_name,
        on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name)
      )
    end)
  end

  defp build_order_by_list(all_fields, value_fields) do
    execution_field_set = MapSet.new(execution_fields())
    value_field_indexes = build_value_field_index_map(value_fields)

    Enum.map(all_fields, fn {field, direction} ->
      if field in execution_field_set do
        {direction, field}
      else
        index = Map.get(value_field_indexes, field)
        alias_name = String.to_atom("v#{index}")
        {direction, dynamic([{^alias_name, v}], v.node_value)}
      end
    end)
  end

  defp build_value_field_index_map(value_fields) do
    value_fields
    |> Enum.with_index()
    |> Map.new(fn {{field, _}, index} -> {field, index} end)
  end

  # Normalize sort fields to support both atom and tuple syntax
  defp normalize_sort_fields(fields) when is_list(fields) do
    Enum.map(fields, fn
      # Atom format: bare atom defaults to :asc
      atom when is_atom(atom) ->
        {atom, :asc}

      # Tuple format: {field, direction}
      {field, direction} when is_atom(field) and direction in [:asc, :desc] ->
        {field, direction}

      # Invalid format
      invalid ->
        raise ArgumentError,
              "Invalid sort field format: #{inspect(invalid)}. Expected atom or {field, :asc/:desc} tuple."
    end)
  end

  defp filter_by_graph_name(query, nil), do: query
  defp filter_by_graph_name(query, graph_name), do: from(e in query, where: e.graph_name == ^graph_name)

  defp filter_by_graph_version(query, nil), do: query
  defp filter_by_graph_version(query, graph_version), do: from(e in query, where: e.graph_version == ^graph_version)

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

  # Database-level filtering for simple value comparisons
  defp add_filters(query, []), do: query |> preload_and_convert()

  defp add_filters(query, value_filters) when is_list(value_filters) do
    # Validate all filters are database-compatible before proceeding
    Enum.each(value_filters, &validate_db_filter/1)

    query
    |> apply_db_value_filters(value_filters)
    |> preload_and_convert()
  end

  # Database-level filtering for counting (no preloading or conversion needed)
  defp add_count_filters(query, []), do: Journey.Repo.aggregate(query, :count, :id)

  defp add_count_filters(query, value_filters) when is_list(value_filters) do
    # Validate all filters are database-compatible before proceeding
    Enum.each(value_filters, &validate_db_filter/1)

    query
    |> apply_db_value_filters(value_filters)
    |> Journey.Repo.aggregate(:count, :id)
  end

  # Validate filters are compatible with database-level filtering
  defp validate_db_filter({node_name, :list_contains, value})
       when is_atom(node_name) and (is_binary(value) or is_integer(value)) do
    :ok
  end

  defp validate_db_filter({node_name, op, value})
       when is_atom(node_name) and op in [:eq, :neq, :lt, :lte, :gt, :gte, :in, :not_in, :contains, :icontains] do
    # Additional validation for the value type
    if primitive_value?(value) do
      :ok
    else
      raise ArgumentError,
            "Unsupported value type for database filtering: #{inspect(value)}. " <>
              "Only strings, numbers, booleans, nil, and lists of primitives are supported."
    end
  end

  defp validate_db_filter({node_name, op})
       when is_atom(node_name) and op in [:is_nil, :is_not_nil, :is_set, :is_not_set],
       do: :ok

  # Crash with clear error message for unsupported filters
  defp validate_db_filter(filter) do
    raise ArgumentError,
          "Unsupported filter for database-level filtering: #{inspect(filter)}. " <>
            "Only simple comparisons on strings, numbers, booleans, nil, and lists of primitives are supported. " <>
            "Custom functions are not supported."
  end

  # Check if a value is a primitive type that can be handled at database level
  defp primitive_value?(value) when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
    do: true

  defp primitive_value?(values) when is_list(values) do
    Enum.all?(values, fn v -> is_binary(v) or is_number(v) or is_boolean(v) or is_nil(v) end)
  end

  defp primitive_value?(_), do: false

  # Escape special characters in LIKE patterns to treat them as literals
  defp escape_like_pattern(pattern) do
    pattern
    # Escape backslash first
    |> String.replace("\\", "\\\\")
    # Escape percent
    |> String.replace("%", "\\%")
    # Escape underscore
    |> String.replace("_", "\\_")
  end

  # Apply database-level value filtering using JOINs and JSONB queries
  defp apply_db_value_filters(query, value_filters) do
    # Split filters by type for different handling strategies
    {existence_filters, comparison_filters} =
      Enum.split_with(value_filters, fn
        {_, op} when op in [:is_nil, :is_not_nil, :is_set, :is_not_set] -> true
        _ -> false
      end)

    # Apply comparison filters with JOINs (leveraging unique execution_id, node_name)
    query_with_comparisons =
      Enum.reduce(comparison_filters, query, fn {node_name, op, value}, acc_query ->
        apply_comparison_filter(acc_query, node_name, op, value)
      end)

    # Apply existence filters using anti-join and inner join patterns
    Enum.reduce(existence_filters, query_with_comparisons, fn {node_name, op}, acc_query ->
      node_name_str = Atom.to_string(node_name)

      case op do
        :is_nil ->
          from(e in acc_query,
            left_join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str,
            where: is_nil(v.id)
          )

        :is_not_nil ->
          from(e in acc_query,
            join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str
          )

        :is_set ->
          from(e in acc_query,
            join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str,
            where: not is_nil(v.set_time)
          )

        :is_not_set ->
          from(e in acc_query,
            left_join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str,
            where: is_nil(v.set_time)
          )
      end
    end)
  end

  # Apply individual comparison filters with direct JSONB conditions
  defp apply_comparison_filter(query, node_name, :eq, value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value == ^value
    )
  end

  defp apply_comparison_filter(query, node_name, :neq, value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value != ^value
    )
  end

  defp apply_comparison_filter(query, node_name, :lt, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric < ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :lt, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') < ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :lte, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric <= ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :lte, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') <= ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :gt, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric > ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :gt, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') > ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :gte, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric >= ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :gte, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') >= ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :in, values) when is_list(values) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value in ^values
    )
  end

  defp apply_comparison_filter(query, node_name, :not_in, values) when is_list(values) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value not in ^values
    )
  end

  defp apply_comparison_filter(query, node_name, :contains, pattern) when is_binary(pattern) do
    escaped_pattern = escape_like_pattern(pattern)
    like_pattern = "%#{escaped_pattern}%"

    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') LIKE ?", v.node_value, v.node_value, ^like_pattern)
    )
  end

  defp apply_comparison_filter(query, node_name, :icontains, pattern) when is_binary(pattern) do
    escaped_pattern = escape_like_pattern(pattern)
    like_pattern = "%#{escaped_pattern}%"

    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') ILIKE ?", v.node_value, v.node_value, ^like_pattern)
    )
  end

  defp apply_comparison_filter(query, node_name, :list_contains, element)
       when is_binary(element) or is_integer(element) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "jsonb_typeof(?) = 'array' AND ? @> ?",
          v.node_value,
          v.node_value,
          ^element
        )
    )
  end

  # Helper to preload data and convert node names to atoms
  defp preload_and_convert(query) do
    from(e in query, preload: [:values, :computations])
    |> Journey.Repo.all()
    |> Enum.map(&convert_node_names_to_atoms/1)
  end

  def find_value_by_name(execution, node_name) when is_atom(node_name) do
    execution.values |> Enum.find(fn value -> value.node_name == node_name end)
  end

  defp backoff_sleep(attempt_count) when is_integer(attempt_count) and attempt_count >= 0 do
    jitter = :rand.uniform(1000)

    min(30_000, 500 * attempt_count)
    |> Kernel.+(jitter)
    |> round()
    |> Process.sleep()
  end

  defp convert_node_names_to_atoms(nil), do: nil

  defp convert_node_names_to_atoms(%Execution{} = execution) do
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

  def convert_values_to_atoms(collection_of_maps, key) do
    collection_of_maps
    |> Enum.map(fn
      nil ->
        nil

      map ->
        Map.update!(map, key, &String.to_atom/1)
    end)
  end

  def convert_all_keys_to_atoms(nil), do: nil

  def convert_all_keys_to_atoms(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
    |> Enum.into(%{})
  end

  def computation_db_to_atoms(nil), do: nil

  def computation_db_to_atoms(computation)
      when is_struct(computation, Journey.Persistence.Schema.Execution.Computation) do
    computation
    |> Map.update!(:node_name, fn n -> String.to_atom(n) end)
    |> Map.update!(:computed_with, &convert_all_keys_to_atoms/1)
  end

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

  def migrate_to_current_graph_if_needed(nil), do: nil

  def migrate_to_current_graph_if_needed(execution) do
    case Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version) do
      nil ->
        # Graph not found in catalog, proceed with original execution
        execution

      current_graph ->
        migrate_to_this_graph_if_needed(execution, current_graph)
    end
  end

  defp migrate_to_this_graph_if_needed(execution, graph) do
    if execution.graph_hash == graph.hash do
      # Hashes match, no migration needed
      execution
    else
      # Execution has no hash (old execution) or hashes differ, migrate
      migrate_to_this_graph(execution, graph)
    end
  end

  defp migrate_to_this_graph(execution, graph) do
    {:ok, migrated_execution} =
      Journey.Repo.transaction(fn repo ->
        # Acquire advisory lock to prevent concurrent migrations of the same execution
        lock_key = :erlang.phash2(execution.id)
        repo.query!("SELECT pg_advisory_xact_lock($1, $2)", [@migration_lock_namespace, lock_key])

        # Reload execution after acquiring lock to get the latest state
        current_execution = load_execution_for_migration(repo, execution.id)

        # Check if migration is still needed (another process might have just done it)
        if current_execution.graph_hash == graph.hash do
          # Already migrated by another process
          current_execution
        else
          # Proceed with migration
          perform_migration(repo, execution.id, current_execution, graph)
        end
      end)

    migrated_execution
  end

  defp perform_migration(repo, execution_id, current_execution, graph) do
    # Find missing nodes
    missing_node_names = find_missing_nodes(current_execution, graph)

    # Add missing nodes if any
    if MapSet.size(missing_node_names) > 0 do
      add_missing_nodes(repo, execution_id, current_execution.revision, graph, missing_node_names)
    end

    # Update execution's graph_hash
    from(e in Execution, where: e.id == ^execution_id)
    |> repo.update_all(set: [graph_hash: graph.hash])

    # Reload and return the migrated execution
    load(execution_id, true, false)
  end

  defp load_execution_for_migration(repo, execution_id) do
    from(e in Execution,
      where: e.id == ^execution_id,
      preload: [:values, :computations]
    )
    |> repo.one!()
    |> convert_node_names_to_atoms()
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

  defp add_missing_nodes(repo, execution_id, revision, graph, missing_node_names) do
    graph.nodes
    |> Enum.filter(fn node ->
      MapSet.member?(missing_node_names, node.name)
    end)
    |> Enum.each(fn graph_node ->
      add_node_records(repo, execution_id, revision, graph_node)
    end)
  end

  defp add_node_records(repo, execution_id, _revision, graph_node) do
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
