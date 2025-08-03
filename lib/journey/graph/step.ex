defmodule Journey.Graph.Step do
  @moduledoc false

  defstruct [:name, :gated_by, :f_compute, :f_on_save, :type, :mutates, :max_retries, :abandon_after_seconds]

  @type t :: %__MODULE__{
          name: atom,
          gated_by: list,
          f_compute: function,
          f_on_save: function | nil,
          type: Journey.Schema.Execution.ComputationType.t(),
          mutates: atom | nil,
          max_retries: pos_integer(),
          abandon_after_seconds: pos_integer()
        }
end
