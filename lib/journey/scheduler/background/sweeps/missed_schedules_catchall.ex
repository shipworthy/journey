defmodule Journey.Scheduler.Background.Sweeps.MissedSchedulesCatchall do
  @moduledoc """
  Catch-all sweep to recover schedules missed due to system downtime.
  Runs at most once every 23 hours, checking via SweepRun records.
  """

  require Logger
  import Ecto.Query
  import Journey.Helpers.Log

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Persistence.Schema.SweepRun

  @batch_size 100
  @lookback_days 7
  @min_hours_between_runs 23

  @doc """
  Sweep for executions with past schedule values and trigger advance() on them.
  Processes in batches to avoid memory issues.
  Returns {execution_count, sweep_run_id} tuple.
  """
  def sweep(execution_id \\ nil) do
    prefix = "[#{mf()}] [#{inspect(self())}]"

    # Check if we should run based on last completed sweep
    if should_run?() do
      Logger.info("#{prefix}: starting missed schedules catch-all sweep")

      sweep_start = System.system_time(:second)
      sweep_run = record_sweep_start(:missed_schedules_catchall, sweep_start)

      try do
        # Compute time boundaries once for stable pagination
        now = System.system_time(:second)
        cutoff_time = now - @lookback_days * 24 * 60 * 60
        recent_boundary = now - 25 * 60

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
    else
      Logger.debug("#{prefix}: skipping - ran recently")
      {0, nil}
    end
  end

  defp should_run?() do
    from(sr in SweepRun,
      where: sr.sweep_type == :missed_schedules_catchall,
      order_by: [desc: sr.started_at],
      limit: 1,
      select: sr.started_at
    )
    |> Journey.Repo.one()
    |> case do
      nil ->
        true

      last_started_at_timestamp ->
        last_started_at_timestamp < System.system_time(:second) - @min_hours_between_runs * 60 * 60
    end
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
