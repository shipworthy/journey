defmodule Journey.Executions do
  @moduledoc false
  alias Journey.Execution
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
          |> Enum.filter(fn %{type: type} -> type in Journey.Execution.ComputationType.values() end)
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

        #         %Execution.Value{
        #   execution: execution,
        #   node_name: "last_updated_at",
        #   node_type: :input,
        #   ex_revision: execution.revision,
        #   set_time: now,
        #   node_value: now
        # }

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
        Journey.load(original_execution)

      {:error, _} ->
        Logger.error("#{prefix}: value not set, transaction rolled back")
        Journey.load(execution)
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

  defp load_value(execution, node_name, monotonic_time_deadline, call_count) do
    prefix = "[#{execution.id}][#{node_name}][#{mf()}][#{call_count}]"

    from(v in Execution.Value,
      where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
    )
    |> Journey.Repo.one()
    |> case do
      nil ->
        Logger.debug("#{prefix}: value not found.")
        {:error, :no_such_value}

      %{set_time: nil} ->
        if monotonic_time_deadline == :infinity or
             (monotonic_time_deadline != nil and monotonic_time_deadline > System.monotonic_time(:millisecond)) do
          Logger.debug("#{prefix}: value not set, waiting, call count: #{call_count}")
          backoff_sleep(call_count)
          load_value(execution, node_name, monotonic_time_deadline, call_count + 1)
        else
          {:error, :not_set}
        end

      %{node_value: node_value} ->
        {:ok, node_value}
    end
  end

  defp load_value_wait_new(execution, node_name, monotonic_time_deadline, call_count) do
    prefix = "[#{execution.id}][#{node_name}][#{mf()}][#{call_count}] wait_new"

    # Get the starting revision from the execution parameter
    starting_value = find_value_by_name(execution, node_name)
    starting_revision = if starting_value, do: starting_value.ex_revision, else: 0

    Logger.debug("#{prefix}: waiting for revision > #{starting_revision}")

    # Query for current value in the database
    current_value =
      from(v in Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
      )
      |> Journey.Repo.one()

    if current_value != nil and current_value.ex_revision > starting_revision do
      # Found a newer revision with a value set
      Logger.debug("#{prefix}: found newer revision #{current_value.ex_revision}")
      {:ok, current_value.node_value}
    else
      # Either no value exists yet OR revision hasn't advanced - keep waiting
      current_revision = if current_value, do: current_value.ex_revision, else: "none"

      if deadline_exceeded?(monotonic_time_deadline) do
        Logger.info(
          "#{prefix}: timeout reached waiting for revision > #{starting_revision} (current: #{current_revision})"
        )

        {:error, :not_set}
      else
        Logger.debug("#{prefix}: revision still #{current_revision}, waiting, call count: #{call_count}")
        backoff_sleep(call_count)
        load_value_wait_new(execution, node_name, monotonic_time_deadline, call_count + 1)
      end
    end
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

  def computation_db_to_atoms(computation) when is_struct(computation, Journey.Execution.Computation) do
    computation
    |> Map.update!(:node_name, fn n -> String.to_atom(n) end)
    |> Map.update!(:computed_with, &convert_all_keys_to_atoms/1)
  end

  def find_computations_by_node_name(execution, node_name) when is_atom(node_name) do
    execution.computations |> Enum.filter(fn c -> c.node_name == node_name end)
  end

  # def convert_key_to_atom(map, key) do
  #   Map.update!(map, key, &String.to_atom/1)
  # end
end
