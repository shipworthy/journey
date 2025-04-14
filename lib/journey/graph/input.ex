defmodule Journey.Graph.Input do
  @moduledoc false

  defstruct [:name, type: :input]
  @type t :: %__MODULE__{name: atom, type: :input}
end
