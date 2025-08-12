defmodule Journey.Executions do
  @moduledoc false
  alias Journey.Persistence.Schema.Execution
  import Ecto.Query

  require Logger
  import Journey.Helpers.Log

  def create_new(graph_name, graph_version, nodes) do
    {:ok, execution} =
      Journey.Repo.transaction(fn repo ->
        execution =
          %Execution{
            graph_name: graph_name,
            graph_version: graph_version,
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
      |> repo.update_all(set: update_params)

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
        Journey.Scheduler.advance(updated_execution)

      {:error, {:no_change, original_execution}} ->
        Logger.debug("#{prefix}: value already unset")
        original_execution

      {:error, _} ->
        Logger.error("#{prefix}: value not unset, transaction rolled back")
        Journey.load(execution)
    end
  end

  # credo:disable-for-lines:10 Credo.Check.Refactor.CyclomaticComplexity
  def set_value(execution_id, node_name, value) when is_binary(execution_id) do
    prefix = "[#{execution_id}] [#{mf()}] [#{node_name}]"
    Logger.debug("#{prefix}: setting value, #{inspect(value)}")

    Journey.Repo.transaction(fn repo ->
      new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution_id, repo)

      current_value_node =
        from(v in Execution.Value,
          where: v.execution_id == ^execution_id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.one!()

      updating_to_the_same_value? =
        current_value_node.set_time != nil and current_value_node.node_value != nil and
          current_value_node.node_value == value

      if updating_to_the_same_value? do
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
    |> handle_set_value_result(execution_id)
  end

  # credo:disable-for-lines:10 Credo.Check.Refactor.CyclomaticComplexity
  def set_value(execution, node_name, value) do
    prefix = "[#{execution.id}] [#{mf()}] [#{node_name}]"
    Logger.debug("#{prefix}: setting value, #{inspect(value)}")

    Journey.Repo.transaction(fn repo ->
      new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution.id, repo)

      current_value_node =
        from(v in Execution.Value,
          where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.one!()

      updating_to_the_same_value? =
        current_value_node.set_time != nil and current_value_node.node_value != nil and
          current_value_node.node_value == value

      if updating_to_the_same_value? do
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

        updated_execution
        |> Journey.Scheduler.advance()

      {:error, {:no_change, original_execution}} ->
        Logger.debug("#{prefix}: value not set (updating for the same value), transaction rolled back")
        original_execution

      {:error, _} ->
        Logger.error("#{prefix}: value not set, transaction rolled back")
        Journey.load(execution)
    end
  end

  defp handle_set_value_result(result, execution_id) do
    prefix = "[#{execution_id}]"

    case result do
      {:ok, updated_execution} ->
        Logger.info("#{prefix}: value set successfully")
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
        nil ->
          nil

        :infinity ->
          :infinity

        ms ->
          System.monotonic_time(:millisecond) + ms
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
      "[#{execution.id}][#{node_name}][#{mf()}][#{call_count}]" <>
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
         %{set_time: set_time, ex_revision: revision, node_value: value} = value_node,
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
        {:ok, value}

      # For regular load_value: return if value is set
      wait_for_revision == nil and set_time != nil ->
        Logger.info("#{prefix}: have value, returning.")
        {:ok, value}

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

  def list(graph_name, sort_by_ex_fields, value_filters, limit, offset, include_archived?)
      when (is_nil(graph_name) or is_binary(graph_name)) and
             is_list(sort_by_ex_fields) and
             is_list(value_filters) and
             is_number(limit) and
             is_number(offset) and
             is_boolean(include_archived?) do
    q = from(e in Execution, limit: ^limit, offset: ^offset)

    q =
      if include_archived? do
        q
      else
        from(e in q, where: is_nil(e.archived_at))
      end

    q =
      sort_by_ex_fields
      |> Enum.reduce(q, fn sort_field, acc ->
        from(e in acc, order_by: [asc: ^sort_field])
      end)

    if graph_name == nil do
      q
    else
      from(e in q, where: e.graph_name == ^graph_name)
    end
    |> add_filters(value_filters)
  end

  def archive_execution(execution_id) do
    prefix = "[#{mf()}][#{execution_id}]"
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
    prefix = "[#{mf()}][#{execution_id}]"
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

  defp add_filters(q, value_filters) do
    from(e in q, preload: [:values, :computations])
    |> Journey.Repo.all()
    |> Enum.map(fn execution -> convert_node_names_to_atoms(execution) end)
    |> Enum.filter(fn execution ->
      Enum.all?(value_filters, fn
        {value_node_name, comparator, value_node_value}
        when is_atom(value_node_name) and
               (comparator in [:eq, :neq, :lt, :lte, :gt, :gte, :in, :not_in] or is_function(comparator)) ->
          value_node = find_value_by_name(execution, value_node_name)
          value_node != nil and cmp(comparator).(value_node.node_value, value_node_value)

        {value_node_name, comparator}
        when is_atom(value_node_name) and
               (comparator in [:is_nil, :is_not_nil] or is_function(comparator)) ->
          value_node = find_value_by_name(execution, value_node_name)
          value_node != nil and cmp(comparator).(value_node.node_value)
      end)
    end)
  end

  defp cmp(:eq), do: fn a, b -> a == b end
  defp cmp(:neq), do: fn a, b -> a != b end
  defp cmp(:lt), do: fn a, b -> a < b end
  defp cmp(:lte), do: fn a, b -> a <= b end
  defp cmp(:gt), do: fn a, b -> a > b end
  defp cmp(:gte), do: fn a, b -> a >= b end
  defp cmp(:in), do: fn a, b -> a in b end
  defp cmp(:not_in), do: fn a, b -> a not in b end
  defp cmp(:is_nil), do: fn a -> is_nil(a) end
  defp cmp(:is_not_nil), do: fn a -> not is_nil(a) end
  defp cmp(f) when is_function(f), do: f

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
end
