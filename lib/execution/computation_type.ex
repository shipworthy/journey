defmodule Journey.Execution.ComputationType do
  @type t :: :unknown | :compute | :mutation | :pulse_once | :pulse_recurring

  @doc """
  Returns all possible types of computations.
  """
  @spec values() :: [t()]

  def values() do
    [
      :compute,
      :mutation,
      :pulse_once,
      :pulse_recurring
    ]
  end
end
