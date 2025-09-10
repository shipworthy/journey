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
  import Journey.Helpers.Log
  import Journey.Scheduler.Background.Sweeps.Helpers

  alias Journey.Persistence.Schema.SweepRun

  @batch_size 100
  @min_seconds_between_runs 30 * 60
  @too_new_threshold_seconds 10 * 60
  @extra_overlap_seconds 3 * 60

  # Advisory lock ID for this sweep type
  @lock_id :erlang.phash2(:stalled_executions_sweep)

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
    prefix = "[#{mf()}] [#{inspect(self())}]"

    # Two-phase check – check before getting a lock and checking again.
    if never_ran_or_enough_time_since_last_sweep?(current_time) do
      case create_new_sweep_record_maybe(current_time) do
        nil ->
          {0, nil}

        new_sweep_record ->
          try do
            total_processed = perform_sweep(execution_id, current_time)
            record_sweep_completion(new_sweep_record, total_processed, current_time)
            Logger.info("#{prefix}: completed. attempted to advance #{total_processed} execution(s)")
            {total_processed, new_sweep_record.id}
          rescue
            e ->
              Logger.error("#{prefix}: error during sweep: #{inspect(e)}")
              reraise e, __STACKTRACE__
          end
      end
    else
      Logger.info("#{prefix}: skipping this run")
      {0, nil}
    end
  end

  defp create_new_sweep_record_maybe(current_time) do
    Journey.Repo.transaction(fn ->
      # Acquire transaction-scoped advisory lock
      query = "SELECT pg_try_advisory_xact_lock($1)"

      case Journey.Repo.query(query, [@lock_id]) do
        {:ok, %{rows: [[true]]}} ->
          # We have the lock, do authoritative check and maybe create record
          # credo:disable-for-next-line Credo.Check.Refactor.Nesting
          if never_ran_or_enough_time_since_last_sweep?(current_time) do
            %SweepRun{}
            |> SweepRun.changeset(%{
              sweep_type: :stalled_executions,
              started_at: current_time,
              completed_at: nil,
              executions_processed: 0
            })
            |> Journey.Repo.insert!()
          else
            nil
          end

        {:ok, %{rows: [[false]]}} ->
          Journey.Repo.rollback({:skip, "another process is already checking/running"})

        error ->
          Journey.Repo.rollback({:skip, "failed to acquire lock: #{inspect(error)}"})
      end
    end)
    |> case do
      {:ok, sweep_run} ->
        sweep_run

      _no_success ->
        nil
    end
  end

  defp never_ran_or_enough_time_since_last_sweep?(current_time) do
    from(sr in SweepRun,
      where: sr.sweep_type == :stalled_executions,
      order_by: [desc: sr.started_at],
      limit: 1
    )
    |> Journey.Repo.one()
    |> case do
      nil ->
        Logger.info("[#{mf()}]: no existing run record")
        true

      last_run_record ->
        enough_time_threshold = current_time - @min_seconds_between_runs
        enough_time_passed? = last_run_record.started_at < enough_time_threshold
        more_or_less = if enough_time_passed?, do: "more", else: "less"

        Logger.info(
          "[#{mf()}]: last run happened at #{to_dt(last_run_record.started_at)}, #{more_or_less} than #{@min_seconds_between_runs} seconds ago"
        )

        enough_time_passed?
    end
  end

  defp to_dt(nil), do: "nil"
  defp to_dt(unix_time_seconds), do: DateTime.from_unix!(unix_time_seconds, :second)

  defp perform_sweep(execution_id, current_time) do
    check_from = compute_check_from_threshold()
    check_to = current_time - @too_new_threshold_seconds
    Logger.info("[#{mf()}]: checking executions updated between #{to_dt(check_from)} and #{to_dt(check_to)}")
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
                Logger.error(
                  "[#{mf()}] Failed to process execution #{exec_id}: #{Exception.format(:error, e, __STACKTRACE__)}"
                )

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

  defp record_sweep_completion(sweep_run, executions_processed, current_time) do
    sweep_run
    |> SweepRun.changeset(%{
      completed_at: current_time,
      executions_processed: executions_processed
    })
    |> Journey.Repo.update!()
  end
end
