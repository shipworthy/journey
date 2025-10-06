defmodule Journey.Scheduler.Background.Sweeps.MissedSchedulesCatchall do
  @moduledoc false
  _ = """
  Catch-all sweep to recover schedules missed due to system downtime.
  Runs at most once every 23 hours, checking via SweepRun records.
  Uses advisory locks to prevent race conditions between multiple processes.

  ## Configuration

  Configure via `config :journey, :missed_schedules_catchall`:

  - `:enabled` - Whether the sweep is enabled (default: true)
  - `:preferred_hour` - Hour of day (0-23) when sweep should run, or nil for no restriction (default: 2)
  - `:lookback_days` - Number of days to look back for missed schedules (default: 7)
  """

  require Logger
  import Ecto.Query
  import Journey.Helpers.Log
  import Journey.Scheduler.Background.Sweeps.Helpers

  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Scheduler.Background.Sweeps.Helpers.Throttle

  @batch_size 100
  @min_hours_between_runs 23
  @recent_boundary_minutes 25

  # Default configuration values
  @default_preferred_hour 2
  @default_lookback_days 7

  @doc false
  def sweep(execution_id \\ nil) do
    _ = """
    Sweep for executions with past schedule values and trigger advance() on them.
    Processes in batches to avoid memory issues.
    Returns {execution_count, sweep_run_id} tuple.
    """

    prefix = "[#{mf()}]"

    # Phase 1: Custom gating checks (enabled, preferred_hour)
    with :ok <- check_sweep_enabled(),
         :ok <- check_preferred_hour() do
      # Phase 2: Use Throttle for time-based gating and lock acquisition
      current_time = System.system_time(:second)
      min_seconds_between_runs = @min_hours_between_runs * 60 * 60

      case Throttle.attempt_to_start_sweep_run(:missed_schedules_catchall, min_seconds_between_runs, current_time) do
        {:ok, sweep_run_id} ->
          Logger.info("#{prefix}: starting missed schedules catch-all sweep")

          try do
            total_processed = perform_sweep(execution_id)
            Throttle.complete_started_sweep_run(sweep_run_id, total_processed, current_time)

            if total_processed == 0 do
              Logger.info("#{prefix}: no executions with missed schedules found")
            else
              Logger.info("#{prefix}: completed. advanced #{total_processed} execution(s)")
            end

            {total_processed, sweep_run_id}
          rescue
            e ->
              Logger.error("#{prefix}: error during sweep: #{inspect(e)}")
              reraise e, __STACKTRACE__
          end

        {:skip, reason} ->
          Logger.info("#{prefix}: skipping - #{reason}")
          {0, nil}
      end
    else
      {:skip, reason} ->
        Logger.info("#{prefix}: skipping - #{reason}")
        {0, nil}
    end
  end

  defp check_sweep_enabled do
    if sweep_enabled?() do
      :ok
    else
      {:skip, "sweep is disabled via configuration"}
    end
  end

  defp check_preferred_hour do
    preferred_hour = get_config(:preferred_hour, @default_preferred_hour)
    current_hour = get_current_hour_utc()

    if preferred_hour == nil or current_hour == preferred_hour do
      :ok
    else
      {:skip, "current hour #{current_hour} != preferred hour #{preferred_hour}"}
    end
  end

  defp perform_sweep(execution_id) do
    lookback_days = get_config(:lookback_days, @default_lookback_days)

    now = System.system_time(:second)
    cutoff_time = now - lookback_days * 24 * 60 * 60
    recent_boundary = now - @recent_boundary_minutes * 60

    process_batches(execution_id, cutoff_time, recent_boundary, 0, 0)
  end

  defp sweep_enabled? do
    get_config(:enabled, true)
  end

  defp get_current_hour_utc do
    {:ok, datetime} = DateTime.from_unix(System.system_time(:second))
    datetime.hour
  end

  defp get_config(key, default) do
    Application.get_env(:journey, :missed_schedules_catchall, [])
    |> Keyword.get(key, default)
  end

  defp process_batches(execution_id, cutoff_time, recent_boundary, offset, total_processed) do
    execution_ids = find_executions_with_past_schedules(execution_id, cutoff_time, recent_boundary, @batch_size, offset)

    case execution_ids do
      [] ->
        total_processed

      ids ->
        processed =
          ids
          |> Enum.reduce(0, fn exec_id, acc ->
            try do
              exec_id
              |> Journey.load()
              |> Journey.Scheduler.advance()

              acc + 1
            rescue
              e ->
                Logger.error("[#{mf()}] Failed to process execution #{exec_id}: #{inspect(e)}")
                acc
            end
          end)

        process_batches(execution_id, cutoff_time, recent_boundary, offset + @batch_size, total_processed + processed)
    end
  end

  defp find_executions_with_past_schedules(execution_id, cutoff_time, recent_boundary, limit, offset) do
    # Get all registered graphs (same pattern as other sweepers)
    all_graphs =
      Journey.Graph.Catalog.list()
      |> Enum.map(fn g -> {g.name, g.version} end)

    # Use executions_for_graphs helper
    from(e in executions_for_graphs(execution_id, all_graphs),
      join: v in Value,
      on: v.execution_id == e.id,
      where:
        v.node_type in [:schedule_once, :schedule_recurring] and
          v.node_value < ^recent_boundary and
          v.node_value > ^cutoff_time,
      distinct: true,
      select: e.id,
      order_by: e.id,
      limit: ^limit,
      offset: ^offset
    )
    |> Journey.Repo.all()
  end
end
