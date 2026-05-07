defmodule Journey.Persistence.Schema.Execution.ComputationType do
  @moduledoc false

  @type t ::
          :unknown
          | :compute
          | :mutate
          | :historian
          | :schedule_once
          | :tick_once
          | :schedule_recurring
          | :tick_recurring
          | :archive
          | :loop

  @doc """
  Returns all possible types of computations.
  """
  @spec values() :: [t()]

  def values() do
    [
      :compute,
      :mutate,
      :historian,
      :schedule_once,
      :tick_once,
      :schedule_recurring,
      :tick_recurring,
      :archive,
      :loop
    ]
  end
end
