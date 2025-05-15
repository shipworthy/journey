defmodule Journey.Execution.ComputationType do
  @moduledoc false

  @type t :: :unknown | :compute | :mutation | :schedule_once | :schedule_recurring

  @doc """
  Returns all possible types of computations.
  """
  @spec values() :: [t()]

  def values() do
    [
      :compute,
      :mutation,
      :schedule_once,
      :schedule_recurring
    ]
  end
end
