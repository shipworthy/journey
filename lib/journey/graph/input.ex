defmodule Journey.Graph.Input do
  @moduledoc false

  defstruct [:name, :f_on_save, type: :input]
  @type t :: %__MODULE__{name: atom, type: :input, f_on_save: function() | nil}
end
