defmodule Journey.Scheduler.BackgroundSweeps do
  @moduledoc false

  require Logger

  import Journey.Helpers.Log

  @mode if Mix.env() != :test, do: :auto, else: :manual

  def child_spec(_arg) do
    Periodic.child_spec(
      id: __MODULE__,
      mode: @mode,
      initial_delay: :timer.seconds(:rand.uniform(20) + 5),
      run: &run/0,
      every: :timer.seconds(5),
      delay_mode: :shifted
    )
  end

  def run() do
    # TODO: replace this with the logic that executes on the same period,
    # regardless of # of replicas.
    Process.flag(:trap_exit, true)
    prefix = "#{mf()}[#{inspect(self())}]"
    Logger.debug("#{prefix}: starting")

    try do
      Logger.debug("#{prefix}: performing sweep")
      Journey.Scheduler.BackgroundSweeps.Abandoned.sweep(nil)
      Journey.Scheduler.BackgroundSweeps.Scheduled.find_and_kick_recently_due_schedule_values(nil)
      Logger.debug("#{prefix}: sweep complete")
    catch
      exception ->
        Logger.error("#{prefix}: #{inspect(exception)}")
    end

    Logger.debug("#{prefix}: done")
  end
end
