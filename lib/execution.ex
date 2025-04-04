defmodule Journey.Execution do
  defstruct [:id, :version, :graph_name, :creation_time, :nodes]

  @type t :: %__MODULE__{
          id: binary,
          version: pos_integer(),
          graph_name: binary,
          creation_time: pos_integer(),
          nodes: map()
        }
end
