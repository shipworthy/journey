defmodule Journey.Scheduler.BackgroundSweeps do
  @moduledoc false

  require Logger

  import Journey.Helpers.Log

  alias Journey.Scheduler.BackgroundSweeps.Abandoned
  alias Journey.Scheduler.BackgroundSweeps.ScheduleOnce
  alias Journey.Scheduler.BackgroundSweeps.ScheduleOnceDownstream

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
      run_sweeps(nil)
    catch
      exception ->
        Logger.error("#{prefix}: #{inspect(exception)}")
    end

    Logger.debug("#{prefix}: done")
  end

  def run_sweeps(execution_id) do
    prefix = "#{mf()}[#{inspect(self())}]"
    Logger.debug("#{prefix}: starting")
    Abandoned.sweep(execution_id)
    ScheduleOnce.sweep(execution_id)
    ScheduleOnceDownstream.sweep(execution_id)
    Logger.debug("#{prefix}: done")
  end

  defp sweep_forever(eid) do
    :timer.sleep(1000)
    Journey.Scheduler.BackgroundSweeps.run_sweeps(eid)
    sweep_forever(eid)
  end

  def start_background_sweeps_in_test(eid) do
    {:ok, background_sweeps_task_pid} = Task.start(fn -> sweep_forever(eid) end)
    background_sweeps_task_pid
  end

  def stop_background_sweeps_in_test(background_sweeps_pid) do
    Process.exit(background_sweeps_pid, :kill)
  end
end
