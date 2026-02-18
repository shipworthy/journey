defmodule Journey.Scheduler.Background.Sweeps.ScheduleNodes do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Scheduler.Background.Sweeps.Helpers
  alias Journey.Scheduler.Background.Sweeps.Helpers.Throttle

  @default_min_seconds_between_runs 120

  @doc false
  def sweep(execution_id, current_time \\ nil)
      when (is_nil(execution_id) or is_binary(execution_id)) and
             (is_nil(current_time) or is_integer(current_time)) do
    # Find and compute all unblocked uncomputed schedule_once computations.
    # Optimized to only process executions updated since last sweep.

    current_time = current_time || System.system_time(:second)

    if get_config(:enabled, true) do
      sweep_impl(execution_id, current_time)
    else
      {0, nil}
    end
  end

  defp sweep_impl(execution_id, current_time) do
    min_seconds = get_config(:min_seconds_between_runs, @default_min_seconds_between_runs)

    case Throttle.attempt_to_start_sweep_run(:schedule_nodes, min_seconds, current_time) do
      {:ok, sweep_run_id} ->
        Logger.info("starting #{execution_id}")

        try do
          kicked_count = perform_sweep_logic(execution_id)
          Throttle.complete_started_sweep_run(sweep_run_id, kicked_count, current_time)

          if kicked_count == 0 do
            Logger.info("no recently due pulse value(s) found")
          else
            Logger.info("completed. kicked #{kicked_count} execution(s)")
          end

          {kicked_count, sweep_run_id}
        rescue
          error ->
            Throttle.complete_started_sweep_run(sweep_run_id, 0, current_time)
            Logger.error("error during sweep: #{inspect(error)}")
            reraise error, __STACKTRACE__
        end

      {:skip, reason} ->
        Logger.info("skipping - #{reason}")
        {0, nil}
    end
  end

  defp get_config(key, default) do
    Application.get_env(:journey, :schedule_nodes_sweep, [])
    |> Keyword.get(key, default)
  end

  defp perform_sweep_logic(execution_id) do
    # Get cutoff time from last completed sweep
    cutoff_time = get_last_sweep_cutoff(:schedule_nodes)

    # Get all registered graphs (same pattern as other sweepers)
    all_graphs =
      Journey.Graph.Catalog.list()
      |> Enum.map(fn g -> {g.name, g.version} end)

    from(c in computations_for_graphs(execution_id, all_graphs),
      join: e in Journey.Persistence.Schema.Execution,
      on: c.execution_id == e.id,
      where:
        c.computation_type in [
          ^:schedule_once,
          ^:tick_once,
          ^:schedule_recurring,
          ^:tick_recurring
        ] and
          c.state == ^:not_set and
          e.updated_at >= ^cutoff_time,
      select: c.execution_id,
      distinct: true
    )
    |> Journey.Repo.all()
    |> Enum.map(fn swept_execution_id ->
      swept_execution_id
      |> Journey.load(computations: [:not_set, :computing])
      |> Journey.Scheduler.advance()
    end)
    |> Enum.count()
  end

  @beginning_of_time_unix 0
  @overlap_buffer_seconds 60

  def get_last_sweep_cutoff(sweep_type) do
    # Get timestamp from last completed sweep, with fallback
    case Throttle.get_last_completed_sweep_time(sweep_type) do
      nil ->
        @beginning_of_time_unix

      last_started_at ->
        last_started_at - @overlap_buffer_seconds
    end
  end
end
