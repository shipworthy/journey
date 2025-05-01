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

        # Create a value record for every graph node, regardless of the graph node's type.
        _values =
          nodes
          |> Enum.map(fn
            graph_node ->
              %Execution.Value{
                execution: execution,
                node_name: Atom.to_string(graph_node.name),
                node_type: graph_node.type,
                set_time: nil,
                node_value: nil
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

            # |> IO.inspect(label: :new_computation)
          end)

        # %Execution{
        #   execution
        #   | values: convert_values_to_atoms(values, :node_name),
        #     computations: convert_values_to_atoms(computations, :node_name)
        # }
        # TODO: investigate if this helps with loading newly updated data (making sure we always get it back), if not -- find a
        #  solution, and do this outside of the transaction.
        load(execution.id)
      end)

    execution
  end

  def load(execution_id, preload? \\ true) when is_binary(execution_id) and is_boolean(preload?) do
    if preload? do
      from(e in Execution, where: e.id == ^execution_id, preload: [:values, :computations])
      |> Journey.Repo.one()
      |> convert_node_names_to_atoms()
    else
      from(e in Execution, where: e.id == ^execution_id)
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
          {:set, if(is_nil(value.node_value), do: nil, else: Map.get(value.node_value, "v"))}
        end

      {value.node_name, node_status}
    end)
    |> Enum.into(%{})
  end

  # credo:disable-for-lines:10 Credo.Check.Refactor.CyclomaticComplexity
  def set_value(execution, node_name, value) do
    prefix = "[#{mf()}][#{execution.id}.#{node_name}]"
    Logger.info("#{prefix}: setting value")

    Journey.Repo.transaction(fn repo ->
      new_revision = Journey.Scheduler.Helpers.increment_execution_revision_in_transaction(execution.id, repo)

      current_value_node =
        from(v in Execution.Value,
          where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.one!()

      updating_to_the_same_value? =
        current_value_node.set_time != nil and current_value_node.node_value != nil and
          Map.has_key?(current_value_node.node_value, "v") and Map.get(current_value_node.node_value, "v") == value

      if updating_to_the_same_value? do
        Logger.info("no need to update, value unchanged, aborting transaction")
        repo.rollback(execution)
      else
        now_seconds = System.system_time(:second)

        from(v in Execution.Value,
          where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.update_all(
          set: [
            ex_revision: new_revision,
            node_value: %{"v" => value},
            updated_at: now_seconds,
            set_time: now_seconds
          ]
        )
        # credo:disable-for-lines:10 Credo.Check.Refactor.Nesting
        |> case do
          {1, _} ->
            Logger.info("#{prefix}: value updated")

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

      {:error, original_execution} ->
        Logger.error("#{prefix}: value not set, transaction rolled back")
        Journey.load(original_execution)
    end
  end

  def get_value(execution, node_name, timeout_ms) do
    prefix = "[#{execution.id}][#{node_name}][#{mf()}]"
    Logger.info("#{prefix}: starting." <> if(timeout_ms != nil, do: " blocking, timeout: #{timeout_ms}", else: ""))

    monotonic_time_deadline =
      case timeout_ms do
        nil ->
          nil

        :infinity ->
          :infinity

        ms ->
          System.monotonic_time(:millisecond) + ms
      end

    load_value(execution, node_name, monotonic_time_deadline, 0)
    |> tap(fn {outcome, _result} ->
      Logger.info("#{prefix}: done. #{inspect(outcome)}")
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
        {:ok, node_value["v"]}
    end
  end

  # note: graph_name could be one of the filter params, when that's implemented.
  # list("chickens graph", [:updated_at, :chicken_name], [{:inserted_at, :eq, "2023-10-01T00:00:00Z"}], limit: 10, page: 1)
  # def list(graph_name, sort_by_fields, filter, opts \\ []) do

  def list(graph_name, sort_by_fields)
      when (is_nil(graph_name) or is_binary(graph_name)) and is_list(sort_by_fields) do
    q = from(e in Execution)

    q =
      sort_by_fields
      |> Enum.reduce(q, fn sort_field, acc ->
        from(e in acc, order_by: [asc: ^sort_field])
      end)

    q =
      if graph_name == nil do
        q
      else
        from(e in q,
          where: e.graph_name == ^graph_name
        )
      end

    from(
      e in q,
      preload: [:values, :computations]
    )
    |> Journey.Repo.all()
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
    %Execution{
      execution
      | values: convert_values_to_atoms(execution.values, :node_name),
        computations: convert_values_to_atoms(execution.computations, :node_name)
    }
  end

  def convert_values_to_atoms(collection_of_maps, key) do
    collection_of_maps
    |> Enum.map(fn map ->
      Map.update!(map, key, &String.to_atom/1)
    end)
  end
end
