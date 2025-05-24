defmodule Journey.Scheduler.BackgroundSweeps.ScheduleOnce do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Helpers.Log
  alias Journey.Execution.Computation

  @doc false
  def sweep(execution_id) when is_nil(execution_id) or is_binary(execution_id) do
    # Find and compute all unblocked uncomputed schedule_once computations.
    # TODO: optimize and scale-ize.
    # v0: brute force -- find schedule_once triggers that have not been computed, kick the execution.
    # v0.1: v0 + only look in executions that are not archived.
    # v0.2: v0.1 + paginate

    prefix = "[#{mf()}] [#{inspect(self())}]"
    Logger.debug("#{prefix}: starting #{execution_id}")

    kicked_count =
      from(c in q_computations(execution_id),
        join: e in Journey.Execution,
        on: c.execution_id == e.id,
        where:
          c.computation_type == ^:schedule_once and
            c.state == ^:not_set and
            is_nil(e.archived_at)
      )
      |> Journey.Repo.all()
      |> Enum.map(fn %{execution_id: execution_id} -> execution_id end)
      |> Enum.uniq()
      |> Enum.map(fn swept_execution_id ->
        swept_execution_id
        |> Journey.load()
        |> Journey.Scheduler.advance()
      end)
      |> Enum.count()

    if kicked_count == 0 do
      Logger.debug("#{prefix}: no recently due pulse value(s) found")
    else
      Logger.debug("#{prefix}: completed. kicked #{kicked_count} execution(s)")
    end
  end

  defp q_computations(nil) do
    from(c in Computation)
  end

  defp q_computations(execution_id) do
    from(c in Computation, where: c.execution_id == ^execution_id)
  end
end
