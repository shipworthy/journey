defmodule Journey.Step do
  @moduledoc ~S"""
  The data structure for defining a step in a process.

  ## Example: Using Journey.Step to Define a Process

      iex> _process = %Journey.Process{
      ...>         process_id: "horoscopes-r-us",
      ...>         steps: [
      ...>           %Journey.Step{name: :first_name},
      ...>           %Journey.Step{name: :birth_month},
      ...>           %Journey.Step{name: :birth_day},
      ...>           %Journey.Step{
      ...>             name: :astrological_sign,
      ...>             func: fn _values ->
      ...>               # Everyone is a Taurus!
      ...>               {:ok, "taurus"}
      ...>             end,
      ...>             blocked_by: [
      ...>               %Journey.BlockedBy{step_name: :birth_month, condition: :provided},
      ...>               %Journey.BlockedBy{step_name: :birth_day, condition: :provided}
      ...>             ]
      ...>           },
      ...>           %Journey.Step{
      ...>             name: :horoscope,
      ...>             func: fn values ->
      ...>               name = values[:first_name].value
      ...>               sign = values[:astrological_sign].value
      ...>               {
      ...>                 :ok,
      ...>                 "#{name}! You are a #{sign}! Now is the perfect time to smash the racist patriarchy!"
      ...>               }
      ...>             end,
      ...>             blocked_by: [
      ...>               %Journey.BlockedBy{step_name: :first_name, condition: :provided},
      ...>               %Journey.BlockedBy{step_name: :astrological_sign, condition: :provided}
      ...>             ]
      ...>           }
      ...>         ]
      ...>        }
  """

  @doc false

  @derive Jason.Encoder
  @enforce_keys [:name]
  defstruct [
    :name,
    func: nil,
    blocked_by: []
    # TODO: add retry policy
  ]

  @typedoc """
  Stores the definition of a process step.

  ## name
  The name of the step, some examples:
    :first_name
    "horoscope"
    "offer_rate"

  ## func
  The function that computes the value for the step.

  The function accepts one parameter, which contains the current state of the execution.

  When the function computes the value, it should return it as part of a tuple: `{:ok, value}`.

  If the function was unable to compute the value, with a retriable error, it should return the tuple `{:retriable, error_details}`.

  Any other value will be treated as a non-retriable error.

  ## blocked_by

  A collection of conditions that must be be true for the computation to take place.

  TODO: examples. make those into functions.

  """
  @type t :: %__MODULE__{
          name: String.t(),
          func: (map() -> {:ok | :retriable | :error, any()}),
          blocked_by: list()
        }
end
