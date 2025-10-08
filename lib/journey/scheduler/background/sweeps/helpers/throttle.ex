defmodule Journey.Scheduler.Background.Sweeps.Helpers.Throttle do
  @moduledoc false
  _ = """
  Shared logic for sweeps that need spacing/gating with advisory locks
  to prevent concurrent execution across multiple replicas.

  Provides two main functions:
  - `attempt_to_start_sweep_run/3` - Attempts to start a new sweep run with time-based gating and advisory locks
  - `complete_started_sweep_run/3` - Records completion of a sweep run

  ## Usage

      def sweep(execution_id, current_time) do
        current_time = current_time || System.system_time(:second)

        case Throttle.attempt_to_start_sweep_run(:my_sweep, 30 * 60, current_time) do
          {:ok, sweep_run_id} ->
            try do
              count = perform_sweep(execution_id, current_time)
              # Complete sweep run with actual count on success
              Throttle.complete_started_sweep_run(sweep_run_id, count, current_time)
              {count, sweep_run_id}
            rescue
              error ->
                # Complete sweep run with 0 count on failure
                Throttle.complete_started_sweep_run(sweep_run_id, 0, current_time)
                Logger.error("Error during sweep")
                reraise error, __STACKTRACE__
            end

          {:skip, reason} ->
            Logger.info("Skipping sweep")
            {0, nil}
        end
      end
  """

  require Logger
  import Ecto.Query

  alias Journey.Persistence.Schema.SweepRun

  @doc """
  Attempts to start a new sweep run with advisory lock and time-based gating.

  Returns `{:ok, sweep_run_id}` if the sweep should proceed, or `{:skip, reason}` if it should be skipped.

  ## Parameters

  - `sweep_type` - The type of sweep (atom matching SweepRun.sweep_type enum)
  - `min_seconds_between_runs` - Minimum seconds required between sweep runs
  - `current_time` - Current timestamp in seconds (Unix epoch)

  ## Gating Logic

  1. Checks if enough time has passed since the last sweep run
  2. Acquires a PostgreSQL advisory lock to prevent concurrent runs
  3. Re-checks timing within the lock (authoritative check)
  4. Creates and returns a SweepRun record if all checks pass

  Advisory locks ensure that only one process across all replicas can start
  this sweep type at a time, preventing race conditions in distributed deployments.
  """

  def attempt_to_start_sweep_run(sweep_type, min_seconds_between_runs, current_time)
      when is_atom(sweep_type) and is_integer(min_seconds_between_runs) and
             is_integer(current_time) do
    prefix = "[#{sweep_type}]"

    # Phase 1: Quick check before acquiring lock
    if time_check_passes?(sweep_type, min_seconds_between_runs, current_time) do
      # Phase 2: Acquire lock and do authoritative check
      case acquire_lock_and_create_record(sweep_type, min_seconds_between_runs, current_time) do
        {:ok, sweep_run} ->
          Logger.info("#{prefix}: created sweep run #{sweep_run.id}")
          {:ok, sweep_run.id}

        {:skip, reason} ->
          {:skip, reason}
      end
    else
      {:skip, format_time_check_failure(sweep_type, min_seconds_between_runs, current_time)}
    end
  end

  @doc """
  Records completion of a sweep run.

  Updates the SweepRun record with completion timestamp and execution count.

  ## Parameters

  - `sweep_run_id` - The ID of the sweep run to complete
  - `executions_processed` - Number of executions processed during the sweep
  - `current_time` - Current timestamp in seconds (Unix epoch)
  """

  def complete_started_sweep_run(sweep_run_id, executions_processed, current_time)
      when is_binary(sweep_run_id) and is_integer(executions_processed) and
             is_integer(current_time) do
    sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)

    Logger.info("[#{sweep_run.sweep_type}]: sweep #{sweep_run_id}, processed #{executions_processed} executions")

    sweep_run
    |> SweepRun.changeset(%{
      completed_at: current_time,
      executions_processed: executions_processed
    })
    |> Journey.Repo.update!()
  end

  @doc """
  Returns the started_at timestamp of the most recent completed sweep run for the given type.

  Only considers sweeps that have a completed_at timestamp set.
  Returns `nil` if no completed sweep has run yet.

  ## Parameters

  - `sweep_type` - The type of sweep (atom matching SweepRun.sweep_type enum)

  ## Examples

      iex> Throttle.get_last_completed_sweep_time(:my_sweep)
      1234567890

      iex> Throttle.get_last_completed_sweep_time(:never_completed_sweep)
      nil
  """
  def get_last_completed_sweep_time(sweep_type) when is_atom(sweep_type) do
    from(sr in SweepRun,
      where: sr.sweep_type == ^sweep_type and not is_nil(sr.completed_at),
      order_by: [desc: sr.completed_at],
      limit: 1,
      select: sr.started_at
    )
    |> Journey.Repo.one()
  end

  # Private functions

  defp time_check_passes?(sweep_type, min_seconds_between_runs, current_time) do
    prefix = "[#{sweep_type}]"

    case get_last_sweep_run(sweep_type) do
      nil ->
        Logger.info("#{prefix}: this is the initial run")
        true

      last_run ->
        enough_time_threshold = current_time - min_seconds_between_runs
        ok_to_continue? = last_run.started_at < enough_time_threshold
        Logger.info("#{prefix}: ok to continue? #{ok_to_continue?}")
        ok_to_continue?
    end
  end

  defp format_time_check_failure(sweep_type, min_seconds_between_runs, current_time) do
    case get_last_sweep_run(sweep_type) do
      nil ->
        "no previous run found, but check failed"

      last_run ->
        seconds_since = current_time - last_run.started_at
        "only #{seconds_since} seconds since last run (min: #{min_seconds_between_runs})"
    end
  end

  defp get_last_sweep_run(sweep_type) do
    from(sr in SweepRun,
      where: sr.sweep_type == ^sweep_type,
      order_by: [desc: sr.started_at],
      limit: 1
    )
    |> Journey.Repo.one()
  end

  defp acquire_lock_and_create_record(sweep_type, min_seconds_between_runs, current_time) do
    # Generate a unique lock ID for this sweep type
    lock_id = :erlang.phash2(sweep_type)

    Journey.Repo.transaction(fn ->
      # Acquire transaction-scoped advisory lock
      case Journey.Repo.query("SELECT pg_try_advisory_xact_lock($1)", [lock_id]) do
        {:ok, %{rows: [[true]]}} ->
          # We have the lock, do authoritative check and maybe create record
          create_sweep_run_if_time_check_passes(sweep_type, min_seconds_between_runs, current_time)

        {:ok, %{rows: [[false]]}} ->
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

  defp create_sweep_run_if_time_check_passes(sweep_type, min_seconds_between_runs, current_time) do
    if time_check_passes?(sweep_type, min_seconds_between_runs, current_time) do
      %SweepRun{}
      |> SweepRun.changeset(%{
        sweep_type: sweep_type,
        started_at: current_time,
        completed_at: nil,
        executions_processed: 0
      })
      |> Journey.Repo.insert!()
    else
      Journey.Repo.rollback({:skip, "another process recently started a sweep"})
    end
  end
end
