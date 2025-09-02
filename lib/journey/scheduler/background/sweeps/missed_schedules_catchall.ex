defmodule Journey.Scheduler.Background.Sweeps.MissedSchedulesCatchall do
  @moduledoc """
  Catch-all sweep to recover schedules missed due to system downtime.
  Runs at most once every 23 hours, checking via SweepRun records.

  ## Configuration

  Configure via `config :journey, :missed_schedules_catchall`:

  - `:enabled` - Whether the sweep is enabled (default: true)
  - `:preferred_hour` - Hour of day (0-23) when sweep should run, or nil for no restriction (default: 2)
  - `:lookback_days` - Number of days to look back for missed schedules (default: 7)
  """

  require Logger
  import Ecto.Query
  import Journey.Helpers.Log

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Persistence.Schema.SweepRun

  @batch_size 100
  @min_hours_between_runs 23
  @recent_boundary_minutes 25

  # Default configuration values
  @default_preferred_hour 2
  @default_lookback_days 7

  @doc """
  Sweep for executions with past schedule values and trigger advance() on them.
  Processes in batches to avoid memory issues.
  Returns {execution_count, sweep_run_id} tuple.
  """
  def sweep(execution_id \\ nil) do
    prefix = "[#{mf()}] [#{inspect(self())}]"

    # Check if we should run based on timing constraints
    case should_run_or_not() do
      :ok ->
        Logger.info("#{prefix}: starting missed schedules catch-all sweep")

        sweep_start = System.system_time(:second)
        sweep_run = record_sweep_start(:missed_schedules_catchall, sweep_start)

        try do
          # Get configuration values
          lookback_days = get_config(:lookback_days, @default_lookback_days)

          # Compute time boundaries once for stable pagination
          now = System.system_time(:second)
          cutoff_time = now - lookback_days * 24 * 60 * 60
          recent_boundary = now - @recent_boundary_minutes * 60

          total_processed = process_batches(execution_id, cutoff_time, recent_boundary, 0, 0)

          record_sweep_completion(sweep_run, total_processed)

          if total_processed == 0 do
            Logger.info("#{prefix}: no executions with missed schedules found")
          else
            Logger.info("#{prefix}: completed. advanced #{total_processed} execution(s)")
          end

          {total_processed, sweep_run.id}
        rescue
          e ->
            Logger.error("#{prefix}: error during sweep: #{inspect(e)}")
            reraise e, __STACKTRACE__
        end

      {:wait, reason} ->
        Logger.debug("#{prefix}: skipping - #{reason}")
        {0, nil}
    end
  end

  defp should_run_or_not() do
    if get_config(:enabled, true) == false do
      {:wait, "sweep is disabled via configuration"}
    else
      preferred_hour = get_config(:preferred_hour, @default_preferred_hour)
      last_run = get_last_run()
      now = System.system_time(:second)

      cond do
        # Check minimum time between runs first
        last_run && last_run > now - @min_hours_between_runs * 60 * 60 ->
          hours_since = div(now - last_run, 3600)
          {:wait, "only #{hours_since} hours since last run (min: #{@min_hours_between_runs})"}

        # Check preferred hour if configured (nil means no restriction)
        preferred_hour != nil && get_current_hour_utc() != preferred_hour ->
          {:wait, "current hour #{get_current_hour_utc()} != preferred hour #{preferred_hour}"}

        # All conditions met
        true ->
          :ok
      end
    end
  end

  defp get_last_run do
    from(sr in SweepRun,
      where: sr.sweep_type == :missed_schedules_catchall,
      order_by: [desc: sr.started_at],
      limit: 1,
      select: sr.started_at
    )
    |> Journey.Repo.one()
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
    from(e in base_executions_query(execution_id),
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

  defp base_executions_query(nil) do
    from(e in Execution, where: is_nil(e.archived_at))
  end

  defp base_executions_query(execution_id) do
    from(e in base_executions_query(nil), where: e.id == ^execution_id)
  end

  defp record_sweep_start(sweep_type, started_at) do
    %SweepRun{}
    |> SweepRun.changeset(%{
      sweep_type: sweep_type,
      started_at: started_at
    })
    |> Journey.Repo.insert!()
  end

  defp record_sweep_completion(sweep_run, executions_processed) do
    sweep_run
    |> SweepRun.changeset(%{
      completed_at: System.system_time(:second),
      executions_processed: executions_processed
    })
    |> Journey.Repo.update!()
  end
end
