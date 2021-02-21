defmodule Journey.ExecutionStore do
  use Agent

  @moduledoc false

  @doc """
  Starts a new execution store.
  """
  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @spec get(String.t()) :: Journey.Execution.t() | nil
  @doc """
  Gets an execution by id.
  """
  def get(execution_id) do
    Agent.get(__MODULE__, fn state ->
      #  IO.inspect(state, label: "get/current state")
      Map.get(state, execution_id)
    end)
  end

  @spec put(%Journey.Execution{}) :: Journey.Execution.t()
  @doc """
  Stores an execution.
  """
  def put(execution) do
    :ok =
      Agent.update(__MODULE__, fn storage ->
        # execution = %{execution | save_version: execution.save_version + 1}
        Map.put(storage, execution.execution_id, execution)
      end)

    get(execution.execution_id)
  end

  @spec update_value(String.t(), atom(), any, any) :: {atom(), Journey.Execution.t()}
  @doc """
  Updates a value in an execution.
  """
  def update_value(execution_id, value_name, expected_status, value) do
    Agent.get_and_update(__MODULE__, fn storage ->
      case storage[execution_id] do
        nil ->
          {{:unknown_execution_id, nil}, storage}

        execution ->
          old_values = execution.values

          case old_values[value_name] do
            nil ->
              {{:unknown_step, execution}, storage}

            current_value ->
              # credo:disable-for-next-line Credo.Check.Refactor.Nesting
              if expected_status in [:any, current_value.status] do
                new_values = Map.put(old_values, value_name, value)
                new_execution = Map.put(execution, :values, new_values)
                new_execution = %{new_execution | save_version: new_execution.save_version + 1}
                new_storage = Map.put(storage, execution_id, new_execution)
                {{:ok, new_execution}, new_storage}
              else
                {{:not_updated_due_to_current_status, execution}, storage}
              end
          end
      end
    end)
  end
end
