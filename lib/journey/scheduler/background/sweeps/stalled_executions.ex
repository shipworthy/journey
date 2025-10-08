defmodule Journey.Scheduler.Background.Sweeps.StalledExecutions do
  @moduledoc false
  _ = """
  Sweep to recover executions that may have stalled due to system crashes
  or power failures before computations could be marked as :computing.

  ## Configuration

  Configure via `config :journey, :stalled_executions_sweep`:

  - `:enabled` - Whether the sweep is enabled (default: true)
  """

  require Logger
  import Ecto.Query
  import Journey.Scheduler.Background.Sweeps.Helpers

  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.Helpers.Throttle

  @batch_size 100
  @min_seconds_between_runs 30 * 60
  @too_new_threshold_seconds 10 * 60
  @extra_overlap_seconds 3 * 60

  @doc false
  def sweep(execution_id \\ nil, current_time \\ nil) do
    _ = """
    Sweep for executions that may have stalled and trigger advance() on them.
    Returns {execution_count, new_sweep_run_id} tuple.
    """

    current_time = current_time || System.system_time(:second)

    if get_config(:enabled, true) do
      sweep_impl(execution_id, current_time)
    else
      {0, nil}
    end
  end

  defp sweep_impl(execution_id, current_time) do
    case Throttle.attempt_to_start_sweep_run(:stalled_executions, @min_seconds_between_runs, current_time) do
      {:ok, sweep_run_id} ->
        try do
          total_processed = perform_sweep(execution_id, current_time)
          Throttle.complete_started_sweep_run(sweep_run_id, total_processed, current_time)
          Logger.info("completed. attempted to advance #{total_processed} execution(s)")
          {total_processed, sweep_run_id}
        rescue
          e ->
            Throttle.complete_started_sweep_run(sweep_run_id, 0, current_time)
            Logger.error("error during sweep: #{inspect(e)}")
            reraise e, __STACKTRACE__
        end

      {:skip, reason} ->
        Logger.info("skipping - #{reason}")
        {0, nil}
    end
  end

  defp perform_sweep(execution_id, current_time) do
    check_from = compute_check_from_threshold()
    check_to = current_time - @too_new_threshold_seconds
    Logger.info("checking executions updated between #{to_dt(check_from)} and #{to_dt(check_to)}")
    process_batches_recursively(execution_id, check_from, check_to, 0, 0)
  end

  defp get_config(key, default) do
    Application.get_env(:journey, :stalled_executions_sweep, [])
    |> Keyword.get(key, default)
  end

  defp process_batches_recursively(execution_id, check_from, check_to, offset, total_processed) do
    execution_ids = find_stalled_executions(execution_id, check_from, check_to, @batch_size, offset)

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
                Logger.error("Failed to process execution #{exec_id}: #{Exception.format(:error, e, __STACKTRACE__)}")
                acc
            end
          end)

        process_batches_recursively(
          execution_id,
          check_from,
          check_to,
          offset + @batch_size,
          total_processed + processed
        )
    end
  end

  defp find_stalled_executions(execution_id, check_from, check_to, limit, offset) do
    # Get all registered graphs (same pattern as Abandoned sweeper)
    all_graphs =
      Journey.Graph.Catalog.list()
      |> Enum.map(fn g -> {g.name, g.version} end)

    # Use executions_for_graphs helper
    from(e in executions_for_graphs(execution_id, all_graphs),
      where:
        e.updated_at >= ^check_from and
          e.updated_at < ^check_to,
      distinct: true,
      select: e.id,
      order_by: e.id,
      limit: ^limit,
      offset: ^offset
    )
    |> Journey.Repo.all()
  end

  defp compute_check_from_threshold() do
    # Get timestamp from last completed sweep, with fallback
    last_completion =
      from(sr in SweepRun,
        where: sr.sweep_type == :stalled_executions and not is_nil(sr.completed_at),
        order_by: [desc: sr.completed_at],
        limit: 1,
        select: sr.started_at
      )
      |> Journey.Repo.one()

    # Use last sweep start time with a bit of extra, or fallback to beginning of time
    if last_completion == nil do
      # Beginning of Unix time
      0
    else
      # give it a bit extra overlap, and make sure to account for the records that were deemed too new last time.
      last_completion -
        @extra_overlap_seconds -
        @too_new_threshold_seconds
    end
  end

  defp to_dt(unix_time_seconds), do: DateTime.from_unix!(unix_time_seconds, :second)
end
