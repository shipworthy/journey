defmodule Journey.Scheduler.Background.Sweeps.Abandoned do
  @moduledoc false

  require Logger
  import Ecto.Query
  alias Journey.Persistence.Schema.Execution.Computation

  import Journey.Helpers.Log

  # This sweeper is processing abandoned computation in batches to avoid exhausting memory.
  @batch_size 100

  @batch_count_to_warn 1000

  def sweep(execution_id) do
    prefix = "[#{mf()}]"
    Logger.info("#{prefix}: starting")

    kicked_count = process_until_done(execution_id, MapSet.new(), 1)

    Logger.info("#{prefix}: done, kicked #{kicked_count} execution(s)")
    kicked_count
  end

  defp process_until_done(execution_id, seen_computation_ids, batch_number) do
    prefix = "[#{if execution_id == nil, do: "all executions", else: execution_id}] [#{mf()}] [batch #{batch_number}]"

    if rem(batch_number, @batch_count_to_warn) == 0 do
      # If we processed a lot of abandoned computations in this sweep, emit a warning. so the operator is aware.
      Logger.warning("#{prefix}: processed #{batch_number * @batch_size} abandoned computations in this sweep")
    end

    {:ok, processed_abandoned_computations} =
      Journey.Repo.transaction(fn repo ->
        find_abandoned_computations_batch(execution_id, repo)
        |> Enum.reject(fn c ->
          # credo:disable-for-next-line Credo.Check.Refactor.Nesting
          if MapSet.member?(seen_computation_ids, c.id) do
            Logger.warning(
              "#{prefix} [#{c.id}]: computation skipped (already handled and probably failed in this sweep)"
            )

            true
          else
            false
          end
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
        process_until_done(execution_id, MapSet.union(seen_computation_ids, computation_ids), batch_number + 1)
    end
  end

  defp kick_all_executions_for_these_computations(computations) do
    computations
    |> Enum.map(fn record -> record.execution_id end)
    |> Enum.uniq()
    |> Enum.map(fn execution_id -> Journey.kick(execution_id) end)
    |> length()
  end

  defp find_abandoned_computations_batch(execution_id, repo) do
    current_epoch_second = System.system_time(:second)

    from(c in from_computations(execution_id),
      join: e in Journey.Persistence.Schema.Execution,
      on: c.execution_id == e.id,
      where:
        c.state == ^:computing and not is_nil(c.deadline) and c.deadline < ^current_epoch_second and
          is_nil(e.archived_at),
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

  defp from_computations(nil) do
    from(c in Computation)
  end

  defp from_computations(execution_id) do
    from(c in Computation, where: c.execution_id == ^execution_id)
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
