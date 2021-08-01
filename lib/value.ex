defmodule Journey.Value do
  @moduledoc """
  The structure containing the value assigned to a step.
  """

  @derive Jason.Encoder
  @doc false
  defstruct [
    :name,
    value: nil,
    update_time: 0,
    status: :not_computed
  ]

  @typedoc """
  Contains the value assigned to a process step.

  ## name
  The name of the step associated with this value.

  Example: `:first_name`.

  ## value

  The actual value.

  Example: `"Mario"`.

  ## update_time

  The time (in Unix Epoch seconds) when the last update took place.

  Example: `1615793585`.

  ## status

  The status of this value.

  Example: `:not_computed`.
  """

  @type t :: %Journey.Value{
          name: String.t(),
          value: any(),
          update_time: integer(),
          status: :not_computed | :computing | :computed | :failed
        }

  def convert_from_string_keys(string_keyed_value) do
    %Journey.Value{
      name: string_keyed_value["name"] |> String.to_existing_atom(),
      value: string_keyed_value["value"],
      update_time: string_keyed_value["update_time"],
      status: string_keyed_value["status"] |> String.to_existing_atom()
    }
  end
end
