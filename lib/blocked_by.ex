defmodule Journey.BlockedBy do
  defstruct [
    :step_name,
    :condition
  ]
end

defmodule Journey.ValueCondition do
  defstruct [
    :condition,
    :value
  ]
end
