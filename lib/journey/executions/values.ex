defmodule Journey.Executions.Values do
  @moduledoc false

  alias Journey.Persistence.Schema.Execution
  import Ecto.Query

  require Logger

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
    starting_value = Journey.Executions.find_value_by_name(execution, node_name)
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

  defp backoff_sleep(attempt_count) when is_integer(attempt_count) and attempt_count >= 0 do
    jitter = :rand.uniform(1000)

    min(30_000, 500 * attempt_count)
    |> Kernel.+(jitter)
    |> round()
    |> Process.sleep()
  end
end
