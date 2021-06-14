defmodule Journey.Execution do
  @moduledoc """
  An execution of a process.

  An execution gets created when you call
  ```elixir
  execution = Journey.Process.execute(process)
  ```

  The execution can then be updated with new values

  ```elixir
  {:ok, updated_execution} = Journey.Execution.update_value(
      execution.execution_id,
      :first_name,
      "Luigi")
  ```

  and its can be introspected.

  ```elixir
  > updated_execution |> Journey.Execution.read_value(:first_name)
  {:computed, "Luigi"}
  ```
  """
  require Logger

  defstruct [
    :execution_id,
    :process,
    values: %{},
    save_version: 0
  ]

  @typedoc """
  Stores the information related to an execution of a process.

  ## execution_id
  The id of this execution.

  ## process

  The process that this execution is running.

  ## values

  The values associated with this execution, in various states of computation.

  ## save_version

  The version of this execution. Every time an execution gets updated, it is persisted, and its save_version value gets incremented.
  """
  @type t :: %Journey.Execution{
          execution_id: String.t(),
          process: Journey.Process,
          values: map(),
          save_version: integer()
        }

  @doc """
  Load an execution from store, given its execution id.
  ```elixir
  execution = Journey.Execution.load!(execution_id)
  ```
  """
  @spec load!(String.t()) :: Journey.Execution.t()
  def load!(execution_id) when is_binary(execution_id) do
    {:ok, execution} = load(execution_id)
    execution
  end

  @doc """
  Load an execution from store, given its execution id.

  ```elixir
  {:ok, execution} = Journey.Execution.load(execution_id)
  ```
  """
  @spec load(String.t()) :: {:error, :no_such_execution} | {:ok, Journey.Execution.t()}
  def load(execution_id) when is_binary(execution_id) do
    case Journey.ExecutionStore.get(execution_id) do
      nil ->
        {:error, :no_such_execution}

      execution ->
        {:ok, execution}
    end
  end

  @doc """
  Update a value in an execution, given the execution or execution id. The new value gets immediately persisted. Any steps that become unblocked by this value will start computing.

  The function returns a tuple that, in case of success, includes the updated execution.

  ```elixir
  {:ok, updated_execution} = Journey.Execution.update_value(execution_id, :first_name, "Luigi")
  {:unknown_step, _} = Journey.Execution.update_value(execution_id, :ssn, "111-11-11")
  {:unknown_execution_id, _} = Journey.Execution.update_value(no_such_execution, :first_name, "Luigi")
  ```
  """
  @spec update_value(String.t(), atom(), any) :: {:ok, Journey.Execution.t()}
  def update_value(execution_id, value_name, new_value) when is_binary(execution_id) do
    value = %Journey.Value{
      name: value_name,
      value: new_value,
      update_time: System.os_time(:second),
      status: :computed
    }

    case Journey.ExecutionStore.update_value(execution_id, value_name, :any, value) do
      {:ok, updated_execution} ->
        kick_off_unblocked_steps(updated_execution)

      {result, execution} ->
        {result, execution}
    end
  end

  @spec update_value(Journey.Execution.t(), atom, any) :: {:ok, Journey.Execution}
  def update_value(execution, value_name, new_value) do
    update_value(execution.execution_id, value_name, new_value)
  end

  @doc """
  Get all the steps in an execution that have yet to receive a value.

  ```elixir
  > Journey.Execution.get_unfilled_steps(execution)
  [
    astrological_sign: %Journey.Value{
      name: :astrological_sign,
      status: :not_computed,
      update_time: 0,
      value: nil
    },
    birth_day: %Journey.Value{
      name: :birth_day,
      status: :not_computed,
      update_time: 0,
      value: nil
    },
    birth_month: %Journey.Value{
      name: :birth_month,
      status: :not_computed,
      update_time: 0,
      value: nil
    },
    first_name: %Journey.Value{
      name: :first_name,
      status: :not_computed,
      update_time: 0,
      value: nil
    },
    horoscope: %Journey.Value{
      name: :horoscope,
      status: :not_computed,
      update_time: 0,
      value: nil
    }
  ]
  """
  @spec get_unfilled_steps(Journey.Execution.t()) :: list
  def get_unfilled_steps(execution) do
    execution
    |> Map.get(:values)
    |> Enum.filter(fn {_, value} -> Map.get(value, :status) != :computed end)
  end

  defp is_step_blocked?(process, values, step_name) do
    get_outstanding_dependencies(process, values, step_name)
    # TODO: we shouldn't have to collect and then count all dependencies to answer this function's question.
    |> Enum.count()
    |> (fn n -> n > 0 end).()
  end

  @doc """
  Get all the steps in the execution that are blocked by other steps.
  ```elixir
  > execution |> Journey.Execution.get_blocked_steps()
  [
    astrological_sign: %Journey.Value{
      name: :astrological_sign,
      status: :not_computed,
      update_time: 0,
      value: nil
    },
    horoscope: %Journey.Value{
      name: :horoscope,
      status: :not_computed,
      update_time: 0,
      value: nil
    }
  ]
  ```
  """
  @spec get_blocked_steps(Journey.Execution.t()) :: list
  def get_blocked_steps(execution) do
    # TODO: return the steps on which we are blocked.
    execution
    |> get_unfilled_steps()
    |> Enum.filter(fn {_, value} ->
      is_step_blocked?(execution.process, execution.values, value.name)
    end)
  end

  @doc """
  Read value from an execution.

  ```elixir
  > execution |> Journey.Execution.read_value(:first_name)
  {:computed, "Mario"}
  ```

  ```elixir
  > execution |> Journey.Execution.read_value(:astrological_sign)
  {:computing, nil}
  ```

  ```elixir
  > execution |> Journey.Execution.read_value(:horoscope_name)
  {:not_computed, nil}
  ```

  ```elixir
  > execution |> Journey.Execution.read_value(:ssn)
  {:error, :unknown_step}
  ```

  """
  @spec read_value(Journey.Execution.t(), atom()) ::
          {:not_computed | :computing | :computed | :failed | :error, atom() | nil | Journey.Execution}
  def read_value(execution, value_name) do
    execution
    |> Map.get(:values)
    |> Map.get(value_name)
    |> case do
      nil ->
        {:error, :unknown_step}

      value ->
        {value.status, value.value}
    end
  end

  defp get_outstanding_dependencies(process, values, task_name) do
    process.steps
    |> Enum.find(fn step -> step.name == task_name end)
    |> Map.get(:blocked_by, [])
    |> Enum.filter(fn {upstream_task_name, condition} ->
      case condition do
        :provided ->
          values[upstream_task_name].status != :computed

        {:value, value} ->
          values[upstream_task_name].status != :computed and
            values[upstream_task_name].value != value
      end
    end)
  end

  defp has_outstanding_dependencies?(process, values, step_name) do
    one_blocking_step =
      process.steps
      |> Enum.find(fn step -> step.name == step_name end)
      |> Map.get(:blocked_by, [])
      |> Enum.find(fn {upstream_task_name, condition} ->
        case condition do
          :provided ->
            values[upstream_task_name].status != :computed

          {:value, value} ->
            values[upstream_task_name].status != :computed and
              values[upstream_task_name].value != value
        end
      end)

    one_blocking_step != nil
  end

  defp kickoff(execution, step) do
    {:ok, _pid} =
      Task.start(fn ->
        Logger.debug("'#{execution.execution_id}'.'#{step.name}': starting computation.")

        result =
          case step.func.(execution.values) do
            {:ok, value} ->
              %Journey.Value{
                name: step.name,
                value: value,
                update_time: System.os_time(:second),
                status: :computed
              }

            error_result ->
              Logger.debug("'#{execution.execution_id}'.'#{step.name}': computation failed. #{error_result}")

              # TODO: implement retry policy.
              %Journey.Value{
                name: step.name,
                value: error_result,
                update_time: System.os_time(:second),
                status: :failed
              }
          end

        execution =
          case Journey.ExecutionStore.update_value(execution.execution_id, step.name, :computing, result) do
            {:ok, execution} ->
              Logger.debug("'#{execution.execution_id}'.'#{step.name}': computation result stored.")
              execution

            {:not_updated_due_to_current_status, execution} ->
              # It looks like the task has been updated elsewhere. Log, and move on.
              Logger.debug("'#{execution.execution_id}'.'#{step.name}': unable to persist, the task is not :computing.")
              execution

            error ->
              # The task will eventually be caught by the timeout logic (e. g. if it's been ":computing" for too long).
              Logger.debug("'#{execution.execution_id}'.'#{step.name}': unable to persist. #{error}")
              execution
          end

        kick_off_unblocked_steps(execution)
      end)

    {:ok, execution}
  end

  @spec kick_off_unblocked_steps(Journey.Execution.t()) :: {:ok, Journey.Execution.t()}
  defp kick_off_unblocked_steps(execution) do
    steps_that_can_be_computed =
      execution.process.steps
      |> Enum.filter(fn step -> execution.values[step.name].status == :not_computed end)
      |> Enum.filter(fn step -> step.func != nil end)
      |> Enum.filter(fn step -> !has_outstanding_dependencies?(execution.process, execution.values, step.name) end)

    case steps_that_can_be_computed do
      [] ->
        {:ok, execution}

      [step | _] ->
        computing_value = %Journey.Value{
          name: step.name,
          value: "computing",
          update_time: System.os_time(:second),
          status: :computing
        }

        execution =
          case Journey.ExecutionStore.update_value(execution.execution_id, step.name, :not_computed, computing_value) do
            {:ok, execution} ->
              {:ok, execution} = kickoff(execution, step)
              execution

            {:not_updated_due_to_current_status, execution} ->
              # It looks like the step has already been picked up by someone else, never mind.
              execution
          end

        # kick off other steps, if any.
        kick_off_unblocked_steps(execution)
    end
  end

  @doc """
  Get all steps in the execution, and their current state.

  ```elixir
  iex(9)> execution |> Journey.Execution.get_all_values
  [
  started_at: [
    status: :computed,
    value: 1615790505,
    self_computing: false,
    blocked_by: []
  ],
  first_name: [
    status: :not_computed,
    value: nil,
    self_computing: false,
    blocked_by: []
  ],
  birth_month: [
    status: :not_computed,
    value: nil,
    self_computing: false,
    blocked_by: []
  ],
  birth_day: [
    status: :not_computed,
    value: nil,
    self_computing: false,
    blocked_by: []
  ],
  astrological_sign: [
    status: :not_computed,
    value: nil,
    self_computing: true,
    blocked_by: [:birth_month, :birth_day]
  ],
  horoscope: [
    status: :not_computed,
    value: nil,
    self_computing: true,
    blocked_by: [:first_name, :astrological_sign]
  ]
  ]
  ```
  """
  @spec get_all_values(Journey.Execution.t()) ::
          list(
            {:not_computed | :computed | :computing | :failed,
             [status: any(), value: any(), self_computing: boolean(), blocked_by: list()]}
          )
  def get_all_values(execution) do
    execution.process.steps
    |> Enum.map(fn step ->
      {
        step.name,
        status: execution.values[step.name].status,
        value: execution.values[step.name].value,
        self_computing: step.func != nil,
        blocked_by:
          get_outstanding_dependencies(execution.process, execution.values, step.name)
          |> Enum.map(fn s -> s |> elem(0) end)
      }
    end)
  end

  @doc """
  Returns a human friendly string containing the summary of the current status of the execution.

  ```elixir
  > execution |> Journey.Execution.get_summary |> IO.puts
  Execution Summary
  Execution ID: keo56rvimq
  Execution started: 2021-03-15 06:41:45Z
  Revision: 1
  All Steps:
  [started_at]: '1615790505'. Blocked by: []. Self-computing: false
  [first_name]: 'not_computed'. Blocked by: []. Self-computing: false
  [birth_month]: 'not_computed'. Blocked by: []. Self-computing: false
  [birth_day]: 'not_computed'. Blocked by: []. Self-computing: false
  [astrological_sign]: 'not_computed'. Blocked by: [birth_month, birth_day]. Self-computing: true
  [horoscope]: 'not_computed'. Blocked by: [first_name, astrological_sign]. Self-computing: true

  :ok
  ```
  """
  @spec get_summary(Journey.Execution.t()) :: String.t()
  def get_summary(execution) do
    """
    Execution Summary
    Execution ID: #{execution.execution_id}
    Execution started: #{execution.values[:started_at].value |> DateTime.from_unix!()}
    Revision: #{execution.save_version}
    All Steps:
    #{execution.process.steps |> Enum.map(fn step -> value = case execution.values[step.name].status do
        :computed -> execution.values[step.name].value
        status -> status |> Atom.to_string()
      end
    
      depends_on = get_outstanding_dependencies(execution.process, execution.values, step.name) |> Enum.map(fn s -> s |> elem(0) |> Atom.to_string() end) |> Enum.join(", ")
    
      self_compute = step.func != nil
    
      " [#{step.name |> Atom.to_string()}]: '#{value}'. Blocked by: [#{depends_on}]. Self-computing: #{self_compute}" end) |> Enum.join("\n")}
    """
  end
end