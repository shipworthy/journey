defmodule Journey.Scheduler.BackgroundSweeps.Kick do
  @moduledoc false
  import Journey.Helpers.Log
  require Logger

  def kick(execution_id) do
    prefix = "[#{execution_id}] [#{mf()}] [#{inspect(self())}]"
    Logger.info("#{prefix}: processing execution")

    execution_id
    |> Journey.load()
    |> Journey.Scheduler.advance()
  end
end
