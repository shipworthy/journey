defmodule Journey.Persistence.Schema.Execution.ComputationState do
  @moduledoc false

  @type t :: :not_set | :computing | :success | :failed | :abandoned | :cancelled

  @doc """
  Returns a list of all possible states for a computation node.
  """
  @spec values() :: [t()]

  def values() do
    [
      :not_set,
      :computing,
      :success,
      :failed,
      :abandoned,
      :cancelled
    ]
  end
end
