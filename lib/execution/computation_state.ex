defmodule Journey.Execution.ComputationState do
  @type t :: :not_set | :computing | :set | :failed | :cancelled

  @doc """
  Returns a list of all possible states for a node.
  """
  @spec values() :: [t()]

  def values() do
    [
      :not_set,
      :computing,
      :set,
      :failed,
      :cancelled
    ]
  end
end
