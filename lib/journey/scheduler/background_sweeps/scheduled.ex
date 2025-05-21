defmodule Journey.Scheduler.BackgroundSweeps.Scheduled do
  require Logger
  import Ecto.Query

  import Journey.Helpers.Log
  alias Journey.Execution.Value

  def sweep(execution_id) do
    prefix = "[#{mf()}] [#{inspect(self())}]"
    Logger.debug("#{prefix}: starting")

    now = System.system_time(:second)
    precision_window_seconds = 10 * 60
    cutoff_time = now - precision_window_seconds

    q = from_values(execution_id)

    kicked_count =
      from(v in q,
        where:
          v.node_type == ^:schedule_once or
            (v.node_type == ^:schedule_recurring and
               fragment("CAST(? AS INTEGER) <= ?", v.node_value, ^now) and
               fragment("CAST(? AS INTEGER) >= ?", v.node_value, ^cutoff_time))
      )
      |> Journey.Repo.all()
      |> Enum.map(fn %{execution_id: execution_id} -> execution_id end)
      |> Enum.uniq()
      |> Enum.map(fn swept_execution_id -> Journey.Scheduler.BackgroundSweeps.Kick.kick(swept_execution_id) end)
      |> Enum.count()

    if kicked_count == 0 do
      Logger.debug("#{prefix}: no recently due pulse value(s) found")
    else
      Logger.debug("#{prefix}: completed. kicked #{kicked_count} execution(s)")
    end
  end

  defp from_values(nil) do
    from(v in Value)
  end

  defp from_values(execution_id) do
    from(v in Value, where: v.execution_id == ^execution_id)
  end
end
