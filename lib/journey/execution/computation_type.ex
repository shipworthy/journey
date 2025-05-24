defmodule Journey.Execution.ComputationType do
  @moduledoc false

  @type t :: :unknown | :compute | :mutate | :schedule_once | :schedule_recurring | :archive

  @doc """
  Returns all possible types of computations.
  """
  @spec values() :: [t()]

  def values() do
    [
      :compute,
      :mutate,
      :schedule_once,
      :schedule_recurring,
      :archive
    ]
  end
end
