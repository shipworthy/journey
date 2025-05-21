defmodule Journey.Scheduler.BackgroundSweeps.Abandoned do
  require Logger
  import Ecto.Query
  alias Journey.Execution.Computation

  import Journey.Helpers.Log

  def sweep(execution_id) do
    find_and_maybe_reschedule(execution_id)
    |> Enum.map(fn %{execution_id: execution_id} -> execution_id end)
    |> Enum.uniq()
    |> Enum.map(fn swept_execution_id -> Journey.Scheduler.BackgroundSweeps.Kick.kick(swept_execution_id) end)
  end

  def find_and_maybe_reschedule(execution_id) do
    prefix = "[#{if execution_id == nil, do: "all executions", else: execution_id}] [#{mf()}]"
    Logger.debug("#{prefix}: starting")

    current_epoch_second = System.system_time(:second)

    {:ok, computations_marked_as_abandoned} =
      Journey.Repo.transaction(fn repo ->
        abandoned_computations =
          from(c in from_computations(execution_id),
            where: c.state == ^:computing and not is_nil(c.deadline) and c.deadline < ^current_epoch_second,
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()
          |> Journey.Executions.convert_values_to_atoms(:node_name)

        abandoned_computations =
          abandoned_computations
          |> filter_out_graphless()

        abandoned_computations
        |> Enum.each(fn ac -> Journey.Scheduler.Retry.maybe_schedule_a_retry(ac, repo) end)

        if abandoned_computations == [] do
          Logger.debug("#{prefix}: no abandoned computation(s) found")
        else
          Logger.info("#{prefix}: found #{Enum.count(abandoned_computations)} abandoned computation(s)")
        end

        abandoned_computations
        |> Enum.map(fn ac ->
          ac
          |> Ecto.Changeset.change(%{
            state: :abandoned,
            completion_time: System.system_time(:second)
          })
          |> repo.update!()
        end)
      end)

    computations_marked_as_abandoned
    |> Enum.map(fn ac ->
      Logger.warning("#{prefix}: processed an abandoned computation, #{ac.execution_id}.#{ac.node_name}.#{ac.id}")
      ac
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
        Logger.error("skipping computation #{c.id} / #{c.execution_id} because of unknown graph")
        false
      end
    end)
  end
end
