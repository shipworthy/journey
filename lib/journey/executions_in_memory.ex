defmodule Journey.ExecutionsInMemory do
  @moduledoc false
  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.InMemory

  require Logger
  import Journey.Helpers.Log

  def create_new(graph_name, graph_version, nodes, graph_hash) do
    execution_id = Journey.Helpers.Random.object_id("EXEC")
    now = System.system_time(:second)

    # Create initial execution struct
    execution = %Execution{
      id: execution_id,
      graph_name: graph_name,
      graph_version: graph_version,
      graph_hash: graph_hash,
      revision: 0,
      archived_at: nil,
      inserted_at: now,
      updated_at: now,
      values: [],
      computations: []
    }

    # Create value records for each node
    values =
      nodes
      |> Enum.map(fn graph_node ->
        {set_time, node_value} =
          case graph_node.name do
            :execution_id -> {now, execution_id}
            :last_updated_at -> {now, now}
            _ -> {nil, nil}
          end

        %Execution.Value{
          id: Journey.Helpers.Random.object_id("VAL"),
          execution_id: execution_id,
          node_name: Atom.to_string(graph_node.name),
          node_type: graph_node.type,
          ex_revision: execution.revision,
          set_time: set_time,
          node_value: node_value,
          inserted_at: now,
          updated_at: now
        }
      end)

    # Create computation records for computable nodes
    computations =
      nodes
      |> Enum.filter(fn %{type: type} -> type in Execution.ComputationType.values() end)
      |> Enum.map(fn computation ->
        %Execution.Computation{
          id: Journey.Helpers.Random.object_id("CMP"),
          execution_id: execution_id,
          node_name: Atom.to_string(computation.name),
          computation_type: computation.type,
          state: :not_set,
          inserted_at: now,
          updated_at: now
        }
      end)

    # Store the complete execution with values and computations
    complete_execution = %{execution | values: values, computations: computations}
    InMemory.store(complete_execution)

    complete_execution
  end

  def load(execution_id, preload?, include_archived?) when is_binary(execution_id) do
    execution = InMemory.fetch(execution_id)

    if execution && (include_archived? || is_nil(execution.archived_at)) do
      if preload? do
        convert_node_names_to_atoms(execution)
      else
        execution
      end
    else
      nil
    end
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
    prefix = "[#{execution.id}] [#{mf()}] [#{node_name}]"
    Logger.debug("#{prefix}: unsetting value")

    # Find the value to unset
    target_value = find_value_by_name(execution, node_name)

    if is_nil(target_value) || is_nil(target_value.set_time) do
      Logger.debug("#{prefix}: no need to update, value already unset")
      execution
    else
      new_revision = execution.revision + 1
      now_seconds = System.system_time(:second)

      # Update the target value to unset it
      updated_values =
        execution.values
        |> Enum.map(fn value ->
          if value.node_name == Atom.to_string(node_name) do
            %{value | ex_revision: new_revision, node_value: nil, updated_at: now_seconds, set_time: nil}
          else
            value
          end
        end)

      # Update last_updated_at
      updated_values =
        updated_values
        |> Enum.map(fn
          %{node_name: "last_updated_at"} = value ->
            %{
              value
              | ex_revision: new_revision,
                node_value: now_seconds,
                updated_at: now_seconds,
                set_time: now_seconds
            }

          value ->
            value
        end)

      updated_execution = %{execution | revision: new_revision, updated_at: now_seconds, values: updated_values}

      InMemory.store(updated_execution)
      Logger.info("#{prefix}: value unset successfully")

      # Trigger invalidation and advancement (similar to PostgreSQL version)
      graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
      Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
      updated_execution = InMemory.fetch(updated_execution.id) |> convert_node_names_to_atoms()
      Journey.Scheduler.advance(updated_execution)
    end
  end

  def unset_values(execution, node_names) when is_list(node_names) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.debug("#{prefix}: unsetting #{length(node_names)} values: #{inspect(node_names)}")

    # Filter out nodes that are already unset
    nodes_to_unset =
      node_names
      |> Enum.uniq()
      |> Enum.filter(fn node_name ->
        value = find_value_by_name(execution, node_name)
        value && value.set_time != nil
      end)

    if Enum.empty?(nodes_to_unset) do
      Logger.debug("#{prefix}: no values to unset, skipping update")
      execution
    else
      Logger.debug("#{prefix}: #{length(nodes_to_unset)} values to unset, proceeding with update")

      new_revision = execution.revision + 1
      now_seconds = System.system_time(:second)

      # Unset all the target nodes
      nodes_to_unset_str = Enum.map(nodes_to_unset, &Atom.to_string/1)
      updated_values =
        execution.values
        |> Enum.map(fn value ->
          if value.node_name in nodes_to_unset_str do
            %{value | set_time: nil, node_value: nil, ex_revision: new_revision, updated_at: now_seconds}
          else
            value
          end
        end)

      # Update last_updated_at
      updated_values =
        updated_values
        |> Enum.map(fn
          %{node_name: :last_updated_at} = value ->
            %{
              value
              | ex_revision: new_revision,
                node_value: now_seconds,
                updated_at: now_seconds,
                set_time: now_seconds
            }

          value ->
            value
        end)

      updated_execution = %{execution | revision: new_revision, updated_at: now_seconds, values: updated_values}

      InMemory.store(updated_execution)
      Logger.info("#{prefix}: values unset successfully, revision: #{updated_execution.revision}")

      # Trigger invalidation and advancement
      graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
      Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
      updated_execution = InMemory.fetch(updated_execution.id) |> convert_node_names_to_atoms()
      Journey.Scheduler.advance(updated_execution)
    end
  end

  def set_value(execution_id, node_name, value) when is_binary(execution_id) do
    execution = InMemory.fetch(execution_id)
    execution = migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    do_set_value(execution, node_name, value)
  end

  def set_value(execution, node_name, value) when is_struct(execution, Execution) do
    execution = migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    do_set_value(execution, node_name, value)
  end

  defp do_set_value(execution, node_name, value) do
    prefix = "[#{execution.id}] [#{mf()}] [#{node_name}]"
    Logger.debug("#{prefix}: setting value, #{inspect(value)}")

    current_value_node = find_value_by_name(execution, node_name)

    updating_to_the_same_value? =
      current_value_node != nil && current_value_node.set_time != nil &&
      current_value_node.node_value != nil && current_value_node.node_value == value

    if updating_to_the_same_value? do
      Logger.debug("#{prefix}: no need to update, value unchanged")
      execution
    else
      new_revision = execution.revision + 1
      now_seconds = System.system_time(:second)

      # Update the target value
      node_name_str = Atom.to_string(node_name)
      updated_values =
        execution.values
        |> Enum.map(fn val ->
          if val.node_name == node_name_str do
            %{val | ex_revision: new_revision, node_value: value, updated_at: now_seconds, set_time: now_seconds}
          else
            val
          end
        end)

      # Update last_updated_at
      updated_values =
        updated_values
        |> Enum.map(fn val ->
          if val.node_name == "last_updated_at" do
            %{val | ex_revision: new_revision, node_value: now_seconds, updated_at: now_seconds, set_time: now_seconds}
          else
            val
          end
        end)

      updated_execution = %{execution | revision: new_revision, updated_at: now_seconds, values: updated_values}

      InMemory.store(updated_execution)
      Logger.info("#{prefix}: value set successfully")

      # Trigger invalidation and advancement
      graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
      Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
      updated_execution = InMemory.fetch(updated_execution.id) |> convert_node_names_to_atoms()
      Journey.Scheduler.advance(updated_execution)
    end
  end

  def set_values(execution, values_map) when is_map(values_map) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.debug("#{prefix}: setting #{map_size(values_map)} values: #{inspect(Map.keys(values_map))}")

    execution = migrate_to_current_graph_if_needed(execution)

    # Validate all node names and values first
    Enum.each(values_map, fn {node_name, _value} ->
      Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    end)

    # Filter out unchanged values
    changed_values = filter_changed_values(execution, values_map)

    if map_size(changed_values) == 0 do
      Logger.debug("#{prefix}: no values changed, skipping update")
      execution
    else
      Logger.debug("#{prefix}: #{map_size(changed_values)} values changed, proceeding with update")

      new_revision = execution.revision + 1
      now_seconds = System.system_time(:second)

      # Update only the changed values
      updated_values =
        execution.values
        |> Enum.map(fn value ->
          if Map.has_key?(changed_values, value.node_name) do
            new_value = Map.get(changed_values, value.node_name)
            %{value | ex_revision: new_revision, node_value: new_value, updated_at: now_seconds, set_time: now_seconds}
          else
            value
          end
        end)

      # Update last_updated_at
      updated_values =
        updated_values
        |> Enum.map(fn
          %{node_name: :last_updated_at} = value ->
            %{
              value
              | ex_revision: new_revision,
                node_value: now_seconds,
                updated_at: now_seconds,
                set_time: now_seconds
            }

          value ->
            value
        end)

      updated_execution = %{execution | revision: new_revision, updated_at: now_seconds, values: updated_values}

      InMemory.store(updated_execution)
      Logger.info("#{prefix}: values set successfully, revision: #{updated_execution.revision}")

      # Trigger invalidation and advancement
      graph = Journey.Graph.Catalog.fetch(updated_execution.graph_name, updated_execution.graph_version)
      Journey.Scheduler.Invalidate.ensure_all_discardable_cleared(updated_execution.id, graph)
      updated_execution = InMemory.fetch(updated_execution.id) |> convert_node_names_to_atoms()
      Journey.Scheduler.advance(updated_execution)
    end
  end

  defp filter_changed_values(execution, values_map) do
    # Create a map of current values for comparison
    current_values =
      execution.values
      |> Enum.filter(fn v -> Map.has_key?(values_map, v.node_name) end)
      |> Map.new(fn v -> {v.node_name, v.node_value} end)

    # Filter to only include values that are actually changing
    values_map
    |> Enum.filter(fn {node_name, new_value} ->
      current_value = Map.get(current_values, node_name)
      current_value != new_value
    end)
    |> Map.new()
  end

  def get_value(execution, node_name, timeout_ms, opts \\ []) do
    prefix = "[#{execution.id}] [#{mf()}] [#{node_name}]"
    wait_new = Keyword.get(opts, :wait_new, false)

    Logger.debug(
      "#{prefix}: starting." <>
        if(timeout_ms != nil, do: " blocking, timeout: #{timeout_ms}", else: "") <>
        if(wait_new, do: " (wait_new: true)", else: "")
    )

    monotonic_time_deadline =
      case timeout_ms do
        nil -> nil
        :infinity -> :infinity
        ms -> System.monotonic_time(:millisecond) + ms
      end

    if wait_new do
      load_value_wait_new(execution, node_name, monotonic_time_deadline, 0)
    else
      load_value(execution, node_name, monotonic_time_deadline, 0)
    end
    |> tap(fn
      {:ok, _result} ->
        Logger.debug("#{prefix}: done. success")

      {outcome, result} ->
        Logger.info("#{prefix}: done. outcome: '#{inspect(outcome)}', result: '#{inspect(result)}'")
    end)
  end

  defp load_value(execution, node_name, monotonic_time_deadline, call_count) do
    load_value_internal(execution, node_name, monotonic_time_deadline, call_count, nil)
  end

  defp load_value_wait_new(execution, node_name, monotonic_time_deadline, call_count) do
    # Get the starting revision from the execution parameter
    starting_value = find_value_by_name(execution, node_name)
    starting_revision = if starting_value, do: starting_value.ex_revision, else: 0

    load_value_internal(execution, node_name, monotonic_time_deadline, call_count, starting_revision)
  end

  defp load_value_internal(execution, node_name, monotonic_time_deadline, call_count, wait_for_revision) do
    wait_new = wait_for_revision != nil

    prefix =
      "[#{execution.id}][#{node_name}][#{mf()}][#{call_count}]" <>
        if(wait_new, do: " wait_new", else: "")

    if wait_new do
      Logger.debug("#{prefix}: waiting for revision > #{wait_for_revision}")
    end

    # Reload execution from store to get latest data
    current_execution = InMemory.fetch(execution.id) |> convert_node_names_to_atoms()
    current_value = find_value_by_name(current_execution, node_name)

    # Handle the different cases
    cond do
      # For wait_new: check if we have a newer revision
      wait_for_revision != nil && current_value && current_value.set_time != nil &&
          current_value.ex_revision > wait_for_revision ->
        Logger.debug("#{prefix}: found newer revision #{current_value.ex_revision}")
        {:ok, current_value.node_value}

      # For regular load_value: return if value is set
      wait_for_revision == nil && current_value && current_value.set_time != nil ->
        Logger.info("#{prefix}: have value, returning.")
        {:ok, current_value.node_value}

      # Value not set (or not new enough for wait_new)
      true ->
        handle_value_not_ready(
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

  defp handle_value_not_ready(
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

  def list(graph_name, graph_version, sort_by_fields, value_filters, limit, offset, include_archived?) do
    # Get all executions from in-memory store
    all_executions = InMemory.list()

    all_executions
    |> filter_by_graph_name(graph_name)
    |> filter_by_graph_version(graph_version)
    |> filter_archived(include_archived?)
    |> add_filters(value_filters)
    |> apply_sorting(sort_by_fields)
    |> apply_pagination(limit, offset)
    |> Enum.map(&convert_node_names_to_atoms/1)
  end

  defp filter_by_graph_name(executions, nil), do: executions

  defp filter_by_graph_name(executions, graph_name) do
    Enum.filter(executions, fn execution -> execution.graph_name == graph_name end)
  end

  defp filter_by_graph_version(executions, nil), do: executions

  defp filter_by_graph_version(executions, graph_version) do
    Enum.filter(executions, fn execution -> execution.graph_version == graph_version end)
  end

  defp filter_archived(executions, true), do: executions

  defp filter_archived(executions, false) do
    Enum.filter(executions, fn execution -> is_nil(execution.archived_at) end)
  end

  defp add_filters(executions, []), do: executions

  defp add_filters(executions, value_filters) do
    Enum.filter(executions, fn execution ->
      Enum.all?(value_filters, fn filter -> apply_filter(execution, filter) end)
    end)
  end

  # Apply individual filter
  defp apply_filter(execution, {node_name, :eq, value}) do
    node_value = get_node_value(execution, node_name)
    node_value == value
  end

  defp apply_filter(execution, {node_name, :neq, value}) do
    node_value = get_node_value(execution, node_name)
    node_value != value
  end

  defp apply_filter(execution, {node_name, :is_nil}) do
    node_value = get_node_value(execution, node_name)
    is_nil(node_value)
  end

  defp apply_filter(execution, {node_name, :is_not_nil}) do
    node_value = get_node_value(execution, node_name)
    !is_nil(node_value)
  end

  # Add more filter implementations as needed...
  defp apply_filter(_execution, filter) do
    # For now, unsupported filters return true (no filtering)
    Logger.warning("Unsupported filter in in-memory backend: #{inspect(filter)}")
    true
  end

  defp get_node_value(execution, node_name) do
    execution.values
    |> Enum.find(fn value -> value.node_name == Atom.to_string(node_name) end)
    |> case do
      %{set_time: nil} -> nil
      %{node_value: value} -> value
      nil -> nil
    end
  end

  defp apply_sorting(executions, []), do: executions

  defp apply_sorting(executions, sort_by_fields) do
    # Convert sort fields and apply sorting
    normalized_fields = normalize_sort_fields(sort_by_fields)

    Enum.sort(executions, fn exec1, exec2 ->
      Enum.reduce_while(normalized_fields, :eq, fn {field, direction}, acc ->
        if acc == :eq do
          val1 = get_sort_value(exec1, field)
          val2 = get_sort_value(exec2, field)

          comparison = compare_values(val1, val2)

          final_comparison = if direction == :desc, do: reverse_comparison(comparison), else: comparison

          {:cont, final_comparison}
        else
          {:halt, acc}
        end
      end) == :lt
    end)
  end

  defp normalize_sort_fields(fields) do
    Enum.map(fields, fn
      atom when is_atom(atom) -> {atom, :asc}
      {field, direction} when is_atom(field) and direction in [:asc, :desc] -> {field, direction}
    end)
  end

  defp get_sort_value(execution, field) do
    # Check if it's an execution field
    case field do
      :inserted_at -> execution.inserted_at
      :updated_at -> execution.updated_at
      :revision -> execution.revision
      :graph_name -> execution.graph_name
      :graph_version -> execution.graph_version
      :archived_at -> execution.archived_at
      _ -> get_node_value(execution, field)
    end
  end

  defp compare_values(val1, val2) do
    cond do
      val1 == val2 -> :eq
      val1 < val2 -> :lt
      true -> :gt
    end
  end

  defp reverse_comparison(:lt), do: :gt
  defp reverse_comparison(:gt), do: :lt
  defp reverse_comparison(:eq), do: :eq

  defp apply_pagination(executions, limit, offset) do
    executions
    |> Enum.drop(offset)
    |> Enum.take(limit)
  end

  def archive_execution(execution_id) do
    prefix = "[#{mf()}][#{execution_id}]"
    Logger.info("#{prefix}: archiving execution")

    execution = InMemory.fetch(execution_id)

    if execution.archived_at != nil do
      Logger.info("#{prefix}: execution already archived (#{execution.archived_at})")
      execution.archived_at
    else
      now = System.system_time(:second)
      new_revision = execution.revision + 1
      Logger.info("#{prefix}: setting archived_at to #{now}")

      updated_execution = %{execution | archived_at: now, updated_at: now, revision: new_revision}

      InMemory.store(updated_execution)
      now
    end
  end

  def unarchive_execution(execution_id) do
    prefix = "[#{mf()}][#{execution_id}]"
    Logger.info("#{prefix}: unarchiving execution")

    execution = InMemory.fetch(execution_id)

    if execution.archived_at == nil do
      Logger.info("#{prefix}: execution not archived, nothing to do")
      :ok
    else
      Logger.info("#{prefix}: setting archived_at property to nil")
      now = System.system_time(:second)
      new_revision = execution.revision + 1

      updated_execution = %{execution | archived_at: nil, updated_at: now, revision: new_revision}

      InMemory.store(updated_execution)
      :ok
    end
  end

  def find_value_by_name(execution, node_name) when is_atom(node_name) do
    execution.values |> Enum.find(fn value -> value.node_name == Atom.to_string(node_name) end)
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
    execution.computations |> Enum.filter(fn c -> c.node_name == Atom.to_string(node_name) end)
  end

  def history(execution_id) do
    execution = InMemory.fetch(execution_id) |> convert_node_names_to_atoms()

    history_of_computations =
      execution.computations
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
      execution.values
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
    # Find missing nodes
    missing_node_names = find_missing_nodes(execution, graph)

    # Add missing nodes if any
    updated_execution =
      if MapSet.size(missing_node_names) > 0 do
        add_missing_nodes(execution, graph, missing_node_names)
      else
        execution
      end

    # Update execution's graph_hash
    final_execution = %{updated_execution | graph_hash: graph.hash}

    InMemory.store(final_execution)
    final_execution
  end

  defp find_missing_nodes(execution, graph) do
    # Get current node names from execution
    existing_node_names =
      execution.values
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

  defp add_missing_nodes(execution, graph, missing_node_names) do
    now = System.system_time(:second)

    # Create new value records for missing nodes
    new_values =
      graph.nodes
      |> Enum.filter(fn node ->
        MapSet.member?(missing_node_names, node.name)
      end)
      |> Enum.map(fn graph_node ->
        %Execution.Value{
          id: Journey.Helpers.Random.object_id("VAL"),
          execution_id: execution.id,
          node_name: Atom.to_string(graph_node.name),
          node_type: graph_node.type,
          ex_revision: 0,
          set_time: nil,
          node_value: nil,
          inserted_at: now,
          updated_at: now
        }
      end)

    # Create new computation records for missing compute nodes
    new_computations =
      graph.nodes
      |> Enum.filter(fn node ->
        MapSet.member?(missing_node_names, node.name) &&
          node.type in Execution.ComputationType.values()
      end)
      |> Enum.map(fn graph_node ->
        %Execution.Computation{
          id: Journey.Helpers.Random.object_id("CMP"),
          execution_id: execution.id,
          node_name: Atom.to_string(graph_node.name),
          computation_type: graph_node.type,
          state: :not_set,
          inserted_at: now,
          updated_at: now
        }
      end)

    %{execution | values: execution.values ++ new_values, computations: execution.computations ++ new_computations}
  end

  defp convert_node_names_to_atoms(nil), do: nil

  defp convert_node_names_to_atoms(execution) do
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
end
