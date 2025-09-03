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

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Persistence.Schema.SweepRun

  @batch_size 100
  @min_hours_between_runs 23
  @recent_boundary_minutes 25

  # Advisory lock ID for this sweep type
  @lock_id :erlang.phash2(:missed_schedules_catchall)

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

    prefix = "[#{mf()}] [#{inspect(self())}]"

    case get_new_sweep_record_maybe() do
      {:ok, sweep_run} ->
        # We got a sweep record, proceed with the sweep
        Logger.info("#{prefix}: starting missed schedules catch-all sweep")

        try do
          total_processed = perform_sweep(execution_id)

          # Update the sweep record with completion info
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

      {:skip, reason} ->
        # No sweep needed, log the reason and exit
        Logger.debug("#{prefix}: skipping - #{reason}")
        {0, nil}
    end
  end

  defp get_new_sweep_record_maybe() do
    # Phase 1: Cheap checks before acquiring lock
    with :ok <- check_sweep_enabled(),
         :ok <- check_preferred_hour(),
         :ok <- quick_recency_check() do
      # All cheap checks passed, now try to acquire lock and create record
      attempt_sweep_record_creation()
    else
      {:skip, reason} -> {:skip, reason}
    end
  end

  defp check_sweep_enabled() do
    if sweep_enabled?() do
      :ok
    else
      {:skip, "sweep is disabled via configuration"}
    end
  end

  defp check_preferred_hour() do
    preferred_hour = get_config(:preferred_hour, @default_preferred_hour)
    current_hour = get_current_hour_utc()

    if preferred_hour == nil or current_hour == preferred_hour do
      :ok
    else
      {:skip, "current hour #{current_hour} != preferred hour #{preferred_hour}"}
    end
  end

  defp quick_recency_check do
    now = System.system_time(:second)

    case get_last_sweep_run() do
      nil ->
        :ok

      %SweepRun{started_at: last_started} ->
        hours_since = div(now - last_started, 3600)

        if last_started > now - @min_hours_between_runs * 60 * 60 do
          {:skip, "only #{hours_since} hours since last run (min: #{@min_hours_between_runs})"}
        else
          :ok
        end
    end
  end

  defp attempt_sweep_record_creation do
    Journey.Repo.transaction(fn ->
      # Acquire transaction-scoped advisory lock
      query = "SELECT pg_try_advisory_xact_lock($1)"

      case Journey.Repo.query(query, [@lock_id]) do
        {:ok, %{rows: [[true]]}} ->
          # We have the lock, do authoritative check and maybe create record
          authoritative_check_and_create()

        {:ok, %{rows: [[false]]}} ->
          # Another process has the lock
          Journey.Repo.rollback({:skip, "another process is already checking/running"})

        error ->
          Journey.Repo.rollback({:skip, "failed to acquire lock: #{inspect(error)}"})
      end
    end)
    |> case do
      {:ok, sweep_run} ->
        {:ok, sweep_run}

      {:error, {:skip, reason}} ->
        {:skip, reason}

      {:error, reason} ->
        {:skip, "transaction failed: #{inspect(reason)}"}
    end
  end

  defp authoritative_check_and_create do
    now = System.system_time(:second)

    case get_last_sweep_run() do
      nil ->
        # No previous run, create the first one
        create_and_return_sweep_run(now)

      %SweepRun{started_at: last_started} ->
        if last_started <= now - @min_hours_between_runs * 60 * 60 do
          # Enough time has passed, create new run
          create_and_return_sweep_run(now)
        else
          # Too recent - another process must have just created one
          Journey.Repo.rollback({:skip, "another process recently started a sweep"})
        end
    end
  end

  defp create_and_return_sweep_run(started_at) do
    %SweepRun{}
    |> SweepRun.changeset(%{
      sweep_type: :missed_schedules_catchall,
      started_at: started_at,
      completed_at: nil,
      executions_processed: 0
    })
    |> Journey.Repo.insert!()

    # Transaction will commit after this, releasing the lock
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

  defp get_last_sweep_run do
    from(sr in SweepRun,
      where: sr.sweep_type == :missed_schedules_catchall,
      order_by: [desc: sr.started_at],
      limit: 1
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

  defp record_sweep_completion(sweep_run, executions_processed) do
    sweep_run
    |> SweepRun.changeset(%{
      completed_at: System.system_time(:second),
      executions_processed: executions_processed
    })
    |> Journey.Repo.update!()
  end
end
