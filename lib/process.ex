defmodule Journey.Process do
  @moduledoc """

  """
  @derive Jason.Encoder
  @enforce_keys [:process_id, :steps]
  defstruct [:process_id, steps: []]

  @typedoc ~S"""
  Holds the definition of a process.

  """

  @type t :: %Journey.Process{process_id: String.t(), steps: list(Journey.Step.t())}

  defp random_string(length) do
    :crypto.strong_rand_bytes(length)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, length)
    |> String.replace(["+", "/"], "m")
  end

  @doc ~S"""
  Starts a new execution of the process.

  ## Example
      iex> process = %Journey.Process{
      ...>  process_id: "horoscopes-r-us",
      ...>  steps: [
      ...>    %Journey.Step{name: :first_name},
      ...>    %Journey.Step{name: :birth_month},
      ...>    %Journey.Step{name: :birth_day},
      ...>    %Journey.Step{
      ...>      name: :astrological_sign,
      ...>      func: fn _values ->
      ...>        # Everyone is a Taurus!
      ...>        {:ok, "taurus"}
      ...>      end,
      ...>      blocked_by: [
      ...>        %Journey.BlockedBy{step_name: :birth_month, condition: :provided},
      ...>        %Journey.BlockedBy{step_name: :birth_day, condition: :provided}
      ...>      ]
      ...>    },
      ...>    %Journey.Step{
      ...>      name: :horoscope,
      ...>      func: fn values ->
      ...>        name = values[:first_name].value
      ...>        sign = values[:astrological_sign].value
      ...>        {
      ...>          :ok,
      ...>          "#{name}! You are a #{sign}! Now is the perfect time to smash the racist patriarchy!"
      ...>        }
      ...>      end,
      ...>      blocked_by: [
      ...>        %Journey.BlockedBy{step_name: :first_name, condition: :provided},
      ...>        %Journey.BlockedBy{step_name: :astrological_sign, condition: :provided}
      ...>      ]
      ...>    }
      ...>  ]
      ...> }
      iex>
      iex> # Start a new execution of the process.
      iex> execution = Journey.Process.execute(process)
      iex>
      iex>
      iex> {:ok, execution} = Journey.Execution.update_value(execution, :first_name, "Luigi")
      iex> {:not_computed, _} = Journey.Execution.read_value(execution, :astrological_sign)
      iex> {:ok, execution} = Journey.Execution.update_value(execution, :birth_month, 4)
      iex> {:ok, execution} = Journey.Execution.update_value(execution, :birth_day, 29)
      iex> :timer.sleep(100) # Give :astrological_sign's function a bit of time to run.
      iex> {:computed, "taurus"} = execution.execution_id |> Journey.Execution.load!() |> Journey.Execution.read_value(:astrological_sign)
      iex> :timer.sleep(200) # Give :horoscope's function a bit of time to run.
      iex> {:computed, horoscope} = execution.execution_id |> Journey.Execution.load!() |> Journey.Execution.read_value(:horoscope)
      iex> horoscope
      "Luigi! You are a taurus! Now is the perfect time to smash the racist patriarchy!"
  """
  @spec execute(Journey.Process.t()) :: Journey.Execution.t()
  def execute(process) do
    process = %{process | steps: [%Journey.Step{name: :started_at}] ++ process.steps}

    Journey.ProcessCatalog.register(process)

    process.steps
    |> Enum.map(fn step ->
      _atom = if is_atom(step.name), do: step.name, else: String.to_atom(step.name)
    end)

    execution =
      %Journey.Execution{
        execution_id: random_string(10),
        process_id: process.process_id,
        values: blank_values(process)
      }
      |> Journey.ExecutionStore.Postgres.put()

    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :started_at, System.os_time(:second))
    execution
  end

  defp blank_values(process) do
    process
    |> Map.get(:steps, [])
    |> Enum.reduce(
      %{},
      fn step, acc ->
        acc
        |> Map.put_new(step.name, %Journey.Value{name: step.name})
      end
    )
  end
end
