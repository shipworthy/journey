defmodule Journey.Scheduler.Background.Sweeps.ScheduleNodes do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Helpers.Log
  import Journey.Scheduler.Background.Sweeps.Helpers
  alias Journey.Persistence.Schema.SweepRun

  @doc false
  def sweep(execution_id) when is_nil(execution_id) or is_binary(execution_id) do
    # Find and compute all unblocked uncomputed schedule_once computations.
    # Optimized to only process executions updated since last sweep.

    prefix = "[#{mf()}] [#{inspect(self())}]"
    Logger.info("#{prefix}: starting #{execution_id}")

    sweep_start = System.os_time(:second)

    # Record sweep start
    sweep_run = record_sweep_start(:schedule_nodes, sweep_start)

    try do
      # Get cutoff time from last completed sweep
      cutoff_time = get_last_sweep_cutoff(:schedule_nodes)

      # Get all registered graphs (same pattern as other sweepers)
      all_graphs =
        Journey.Graph.Catalog.list()
        |> Enum.map(fn g -> {g.name, g.version} end)

      kicked_count =
        from(c in computations_for_graphs(execution_id, all_graphs),
          join: e in Journey.Persistence.Schema.Execution,
          on: c.execution_id == e.id,
          where:
            c.computation_type in [^:schedule_once, ^:schedule_recurring] and
              c.state == ^:not_set and
              e.updated_at >= ^cutoff_time,
          select: c.execution_id,
          distinct: true
        )
        |> Journey.Repo.all()
        |> Enum.map(fn swept_execution_id ->
          swept_execution_id
          |> Journey.load()
          |> Journey.Scheduler.advance()
        end)
        |> Enum.count()

      # Record sweep completion
      record_sweep_completion(sweep_run, kicked_count)

      if kicked_count == 0 do
        Logger.info("#{prefix}: no recently due pulse value(s) found")
      else
        Logger.info("#{prefix}: completed. kicked #{kicked_count} execution(s)")
      end

      {kicked_count, sweep_run.id}
    rescue
      error ->
        Logger.error("#{prefix}: error during sweep: #{inspect(error)}")
        # Don't record completion on error
        reraise error, __STACKTRACE__
    end
  end

  @beginning_of_time_unix 0
  @overlap_buffer_seconds 60

  def get_last_sweep_cutoff(sweep_type) do
    # Get timestamp from last completed sweep, with fallback
    last_completion =
      from(sr in SweepRun,
        where: sr.sweep_type == ^sweep_type and not is_nil(sr.completed_at),
        order_by: [desc: sr.completed_at],
        limit: 1,
        select: sr.started_at
      )
      |> Journey.Repo.one()

    # Use last sweep start time, or fallback to beginning of time
    if last_completion == nil do
      @beginning_of_time_unix
    else
      last_completion - @overlap_buffer_seconds
    end
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
      completed_at: System.os_time(:second),
      executions_processed: executions_processed
    })
    |> Journey.Repo.update!()
  end
end
