defmodule Journey.Scheduler.BackgroundSweeps.ScheduleOnceDownstream do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Helpers.Log
  alias Journey.Execution
  alias Journey.Execution.Value

  # TODO: the window value should be a property of the computation, settable by the application.
  @rolling_window_seconds 60 * 60

  defp q_execution_ids_to_advance(execution_id) do
    now = System.system_time(:second)
    cutoff_time = now - @rolling_window_seconds

    from(e in q_executions(execution_id),
      join: c in assoc(e, :computations),
      join: v in Value,
      on:
        v.execution_id == e.id and
          v.node_name == c.node_name and
          v.node_type == c.computation_type,
      where:
        c.computation_type in [:schedule_once, :schedule_recurring] and
          c.state == :success and
          not is_nil(v.set_time) and
          (v.node_value <= ^now or
             fragment("?::bigint", v.node_value) <= ^now) and
          v.set_time >= ^cutoff_time,
      # TODO: consider only including executions that have un-computed computations.
      distinct: true,
      select: e.id
    )
  end

  @doc false
  def sweep(execution_id) when is_nil(execution_id) or is_binary(execution_id) do
    # Find and compute all un-computed computations that are downstream of computed schedule_once computations, that are due within the scheduled time window

    prefix = "[#{mf()}] [#{inspect(self())}]"
    Logger.debug("#{prefix}: starting #{execution_id}")

    kicked_count =
      q_execution_ids_to_advance(execution_id)
      |> Journey.Repo.all()
      |> Enum.map(fn swept_execution_id ->
        swept_execution_id
        |> Journey.load()
        |> Journey.Scheduler.advance()
      end)
      |> Enum.count()

    if kicked_count == 0 do
      Logger.debug("#{prefix}: no recently due pulse value(s) found")
    else
      Logger.info("#{prefix}: completed. kicked #{kicked_count} execution(s)")
    end
  end

  defp q_executions(nil) do
    from(e in Execution, where: is_nil(e.archived_at))
  end

  defp q_executions(execution_id) do
    from(e in q_executions(nil), where: e.id == ^execution_id)
  end
end
