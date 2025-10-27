defmodule Journey.Graph.Step do
  @moduledoc false

  defstruct [
    :name,
    :gated_by,
    :f_compute,
    :f_on_save,
    :type,
    :mutates,
    :update_revision_on_change,
    :max_retries,
    :abandon_after_seconds
  ]

  @type t :: %__MODULE__{
          name: atom,
          gated_by: list,
          f_compute: function,
          f_on_save: function | nil,
          type: Journey.Persistence.Schema.Execution.ComputationType.t(),
          mutates: atom | nil,
          update_revision_on_change: boolean,
          max_retries: pos_integer(),
          abandon_after_seconds: pos_integer()
        }
end
