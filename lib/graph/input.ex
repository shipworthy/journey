defmodule Journey.Graph.Input do
  defstruct [:name, type: :input]
  @type t :: %__MODULE__{name: atom, type: :input}
end
