defmodule Journey.Scheduler.Background.Periodic do
  @moduledoc false

  require Logger

  import Journey.Helpers.Log

  alias Journey.Scheduler.Background.Sweeps.Abandoned
  alias Journey.Scheduler.Background.Sweeps.RegenerateScheduleRecurring
  alias Journey.Scheduler.Background.Sweeps.ScheduleNodes
  alias Journey.Scheduler.Background.Sweeps.UnblockedBySchedule

  @mode if Mix.env() != :test, do: :auto, else: :manual

  @sweeper_period_seconds 5 * 60

  def sweeper_period_seconds, do: @sweeper_period_seconds

  def child_spec(_arg) do
    Periodic.child_spec(
      id: __MODULE__,
      mode: @mode,
      initial_delay: :timer.seconds(:rand.uniform(20) + 5),
      run: &run/0,
      every: :timer.seconds(sweeper_period_seconds()),
      delay_mode: :shifted
    )
  end

  def run() do
    Process.flag(:trap_exit, true)
    prefix = "#{mf()}[#{inspect(self())}]"
    Logger.info("#{prefix}: starting, sweeper_period: #{sweeper_period_seconds()} s")

    try do
      run_sweeps(nil)
    catch
      exception ->
        Logger.error("#{prefix}: #{inspect(exception)}")
    end

    Logger.info("#{prefix}: done")
  end

  def run_sweeps(execution_id) do
    prefix = "#{mf()}[#{inspect(self())}]"
    Logger.debug("#{prefix}: starting sweeps for execution_id: #{inspect(execution_id)}")
    Abandoned.sweep(execution_id)
    {_kicked_count, _sweep_run_id} = ScheduleNodes.sweep(execution_id)
    UnblockedBySchedule.sweep(execution_id, sweeper_period_seconds())
    RegenerateScheduleRecurring.sweep(execution_id)
    Logger.debug("#{prefix}: done")
  end

  def start_background_sweeps_in_test(eid) do
    {:ok, background_sweeps_task_pid} = Task.start(fn -> sweep_forever(eid) end)
    background_sweeps_task_pid
  end

  def stop_background_sweeps_in_test(background_sweeps_pid) do
    Process.exit(background_sweeps_pid, :kill)
  end

  defp sweep_forever(eid) do
    :timer.sleep(500)
    Journey.Scheduler.Background.Periodic.run_sweeps(eid)
    sweep_forever(eid)
  end
end
