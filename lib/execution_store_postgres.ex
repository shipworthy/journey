defmodule Journey.ExecutionStore.Postgres do
  @moduledoc false

  require Logger
  import Ecto.Query

  @spec get(String.t()) :: Journey.Execution.t() | nil
  def get(execution_id) do
    Logger.debug("get: #{execution_id}")

    Journey.Repo.get!(Journey.ExecutionDbRecord, execution_id)
    |> Map.get(:execution_data)
  end

  @doc """
  Stores an execution.
  """
  @spec put(%Journey.Execution{}) :: Journey.Execution.t()
  def put(execution) do
    execution = %{execution | save_version: execution.save_version + 1}
    Logger.debug("put: #{execution.execution_id} version #{execution.save_version}")

    {:ok, _execution_db_record} =
      %Journey.ExecutionDbRecord{id: execution.execution_id, execution_data: execution}
      |> Journey.Repo.insert()

    execution
  end

  @spec update_value(String.t(), atom(), atom(), any) :: {atom(), Journey.Execution.t()}
  def update_value(execution_id, step_name, expected_status, value) do
    Logger.debug("update_value: #{execution_id}")

    {:ok, result} =
      Journey.Repo.transaction(fn repo ->
        execution_db_record =
          from(i in Journey.ExecutionDbRecord,
            where: i.id == ^execution_id,
            lock: "FOR UPDATE"
          )
          |> repo.one!()

        execution = Journey.ExecutionDbRecord.convert_to_execution_struct!(execution_db_record.execution_data)
        record_status = execution[:values][step_name].status

        case expected_status do
          s when s in [record_status, :any] ->
            old_values = execution.values
            new_values = Map.put(old_values, step_name, value)
            new_execution = Map.put(execution, :values, new_values)
            new_execution = %{new_execution | save_version: new_execution.save_version + 1}

            execution_db_record
            |> Ecto.Changeset.change(execution_data: new_execution)
            |> repo.update!()

            {:ok, new_execution}

          _ ->
            repo.rollback({:not_updated_due_to_current_status, execution})
        end
      end)

    result
  end
end
