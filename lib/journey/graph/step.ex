defmodule Journey.Graph.Step do
  @moduledoc false

  # TODO: rename upstream_nodes to upstream_conditions
  defstruct [:name, :upstream_nodes, :f_compute, :f_on_save, :type, :mutates, :max_retries, :abandon_after_seconds]

  @type t :: %__MODULE__{
          name: atom,
          upstream_nodes: list,
          f_compute: function,
          f_on_save: function | nil,
          type: Journey.Execution.ComputationType.t(),
          mutates: atom | nil,
          max_retries: pos_integer(),
          abandon_after_seconds: pos_integer()
        }
end
