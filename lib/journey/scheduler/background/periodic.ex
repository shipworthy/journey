defmodule Journey.Scheduler.Background.Periodic do
  @moduledoc """
  Background periodic sweeper that runs all Journey sweeps at a regular interval.

  ## Configuration

  Configure via `config :journey, :background_sweeper`:

  Example:

  config.exs:

  ```
  config :journey, :background_sweeper, period_seconds: 60
  ```

  `period_seconds` is the number seconds between sweep runs. Default: 60 seconds.
  """

  require Logger

  alias Journey.Scheduler.Background.Sweeps.Abandoned
  alias Journey.Scheduler.Background.Sweeps.MissedSchedulesCatchall
  alias Journey.Scheduler.Background.Sweeps.RegenerateScheduleRecurring
  alias Journey.Scheduler.Background.Sweeps.ScheduleNodes
  alias Journey.Scheduler.Background.Sweeps.StalledExecutions
  alias Journey.Scheduler.Background.Sweeps.UnblockedBySchedule

  @mode if Mix.env() != :test, do: :auto, else: :manual

  @default_sweeper_period_seconds 60

  def sweeper_period_seconds do
    period_seconds =
      Application.get_env(:journey, :background_sweeper, [])
      |> Keyword.get(:period_seconds, @default_sweeper_period_seconds)

    Logger.debug("period seconds: #{period_seconds}")
    period_seconds
  end

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
    Logger.debug("starting")

    try do
      run_sweeps(nil)
    catch
      exception ->
        Logger.error("#{inspect(exception)}")
    end

    Logger.debug("done")
  end

  def run_sweeps(execution_id) do
    Logger.debug("starting sweeps for execution_id: #{inspect(execution_id)}")
    {_kicked_count, _sweep_run_id} = Abandoned.sweep(execution_id)
    {_kicked_count, _sweep_run_id} = ScheduleNodes.sweep(execution_id)
    UnblockedBySchedule.sweep(execution_id, sweeper_period_seconds())
    RegenerateScheduleRecurring.sweep(execution_id)
    {_kicked_count, _sweep_run_id} = MissedSchedulesCatchall.sweep(execution_id)
    {_kicked_count, _sweep_run_id} = StalledExecutions.sweep(execution_id)

    Logger.debug("done")
  end

  def start_background_sweeps_in_test(eid) do
    {:ok, background_sweeps_task_pid} = Task.start(fn -> sweep_forever(eid) end)
    background_sweeps_task_pid
  end

  def stop_background_sweeps_in_test(background_sweeps_pid) do
    Process.exit(background_sweeps_pid, :kill)
  end

  def log_configuration do
    period = sweeper_period_seconds()
    abandoned = Application.get_env(:journey, :abandoned_sweep, [])
    schedule_nodes = Application.get_env(:journey, :schedule_nodes_sweep, [])
    stalled = Application.get_env(:journey, :stalled_executions_sweep, [])
    missed = Application.get_env(:journey, :missed_schedules_catchall, [])

    Logger.info("""
    Background sweeper configuration:
      sweep interval: #{period}s
      abandoned: enabled=#{Keyword.get(abandoned, :enabled, true)}, min_seconds_between_runs=#{Keyword.get(abandoned, :min_seconds_between_runs, 59)}
      schedule_nodes: enabled=#{Keyword.get(schedule_nodes, :enabled, true)}, min_seconds_between_runs=#{Keyword.get(schedule_nodes, :min_seconds_between_runs, 120)}
      stalled_executions: enabled=#{Keyword.get(stalled, :enabled, true)}, min_seconds_between_runs=1800 (hardcoded)
      missed_schedules_catchall: enabled=#{Keyword.get(missed, :enabled, true)}, preferred_hour=#{inspect(Keyword.get(missed, :preferred_hour, 2))}, lookback_days=#{Keyword.get(missed, :lookback_days, 7)}
      unblocked_by_schedule: always enabled
      regenerate_recurring: always enabled\
    """)
  end

  defp sweep_forever(eid) do
    :timer.sleep(500)
    Journey.Scheduler.Background.Periodic.run_sweeps(eid)
    sweep_forever(eid)
  end
end
