defmodule Journey.Scheduler.Background.Sweeps.Abandoned do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Helpers.Log
  import Journey.Scheduler.Background.Sweeps.Helpers

  alias Journey.Scheduler.Background.Sweeps.Helpers.Throttle

  # This sweeper is processing abandoned computation in batches to avoid exhausting memory.
  @batch_size 100

  @batch_count_to_warn 1000

  @default_min_seconds_between_runs 59

  def sweep(execution_id, current_time \\ nil) do
    current_time = current_time || System.system_time(:second)

    if get_config(:enabled, true) do
      sweep_impl(execution_id, current_time)
    else
      {0, nil}
    end
  end

  defp sweep_impl(execution_id, current_time) do
    prefix = "[#{mf()}]"
    min_seconds = get_config(:min_seconds_between_runs, @default_min_seconds_between_runs)

    case Throttle.attempt_to_start_sweep_run(:abandoned, min_seconds, current_time) do
      {:ok, sweep_run_id} ->
        Logger.info("#{prefix}: starting")

        try do
          kicked_count = perform_sweep_logic(execution_id, current_time)
          Throttle.complete_started_sweep_run(sweep_run_id, kicked_count, current_time)
          Logger.info("#{prefix}: done, kicked #{kicked_count} execution(s)")
          {kicked_count, sweep_run_id}
        rescue
          e ->
            Logger.error("#{prefix}: error during sweep: #{inspect(e)}")
            reraise e, __STACKTRACE__
        end

      {:skip, reason} ->
        Logger.info("#{prefix}: skipping - #{reason}")
        {0, nil}
    end
  end

  defp get_config(key, default) do
    Application.get_env(:journey, :abandoned_sweep, [])
    |> Keyword.get(key, default)
  end

  defp perform_sweep_logic(execution_id, current_time) do
    process_until_done(execution_id, MapSet.new(), 1, current_time)
  end

  defp process_until_done(execution_id, seen_computation_ids, batch_number, current_time) do
    prefix = "[#{mf()}] [batch #{batch_number}] [#{if execution_id == nil, do: "all executions", else: execution_id}]"

    if rem(batch_number, @batch_count_to_warn) == 0 do
      # If we processed a lot of abandoned computations in this sweep, emit a warning, so that the operator is aware.
      Logger.warning("#{prefix}: evaluated #{batch_number * @batch_size} abandoned computations in this sweep")
    end

    {:ok, processed_abandoned_computations} =
      Journey.Repo.transaction(fn repo ->
        find_abandoned_computations_batch(execution_id, repo, current_time)
        |> Enum.reject(fn c ->
          seen? = MapSet.member?(seen_computation_ids, c.id)

          # credo:disable-for-next-line Credo.Check.Refactor.Nesting
          if seen?,
            do:
              Logger.warning(
                "#{prefix} [#{c.id}]: computation skipped (already handled and probably failed in this sweep)"
              )

          seen?
        end)
        |> process_computations(repo, prefix)
      end)

    kicked_count = kick_all_executions_for_these_computations(processed_abandoned_computations)
    computation_ids = Enum.map(processed_abandoned_computations, fn record -> record.id end) |> MapSet.new()

    if computation_ids == MapSet.new() do
      Logger.info("#{prefix}: no abandoned computations found")
      0
    else
      kicked_count +
        process_until_done(
          execution_id,
          MapSet.union(seen_computation_ids, computation_ids),
          batch_number + 1,
          current_time
        )
    end
  end

  defp kick_all_executions_for_these_computations(computations) do
    computations
    |> Enum.map(fn record -> record.execution_id end)
    |> Enum.uniq()
    |> Enum.map(fn execution_id -> Journey.kick(execution_id) end)
    |> length()
  end

  defp find_abandoned_computations_batch(execution_id, repo, current_time) do
    all_graphs =
      Journey.Graph.Catalog.list()
      |> Enum.map(fn g -> {g.name, g.version} end)

    from(c in computations_for_graphs(execution_id, all_graphs),
      join: e in Journey.Persistence.Schema.Execution,
      on: c.execution_id == e.id,
      where: c.state == :computing and not is_nil(c.deadline) and c.deadline < ^current_time,
      limit: ^@batch_size,
      lock: "FOR UPDATE"
    )
    |> repo.all()
    |> Journey.Executions.convert_values_to_atoms(:node_name)
    |> filter_out_graphless()
  end

  defp process_computations(computations, repo, prefix) do
    # Schedule retries
    computations
    |> Enum.each(fn ac ->
      Journey.Scheduler.Retry.maybe_schedule_a_retry(ac, repo)
    end)

    # Mark as abandoned and return
    computations
    |> Enum.map(fn ac ->
      updated =
        ac
        |> Ecto.Changeset.change(%{
          state: :abandoned,
          completion_time: System.system_time(:second)
        })
        |> repo.update!()

      Logger.info(
        "#{prefix}: processed an abandoned computation, #{updated.execution_id}.#{updated.node_name}.#{updated.id}"
      )

      updated
    end)
  end

  defp filter_out_graphless(computations) do
    # Filter out computations for which there are no graph definitions in the system.
    all_execution_ids =
      computations
      |> Enum.map(fn ac -> ac.execution_id end)
      |> Enum.uniq()

    known_graphs =
      all_execution_ids
      |> Enum.reduce(%{}, fn execution_id, acc ->
        Map.put(acc, execution_id, Journey.Scheduler.Helpers.graph_from_execution_id(execution_id) != nil)
      end)

    computations
    |> Enum.filter(fn c ->
      if Map.get(known_graphs, c.execution_id) == true do
        true
      else
        Logger.info("skipping computation #{c.id} / #{c.execution_id} because of unknown graph")
        false
      end
    end)
  end
end
