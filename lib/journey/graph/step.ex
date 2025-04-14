defmodule Journey.Graph.Step do
  @moduledoc false

  defstruct [:name, :upstream_nodes, :f_compute, :type, :max_retries, :abandon_after_seconds]

  @type t :: %__MODULE__{
          name: atom,
          upstream_nodes: list,
          f_compute: function,
          type: Journey.Execution.ComputationType.t(),
          max_retries: pos_integer(),
          abandon_after_seconds: pos_integer()
        }
end
