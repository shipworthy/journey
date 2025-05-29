defmodule Journey.Scheduler.BackgroundSweeps.UnblockedBySchedule do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Helpers.Log
  alias Journey.Execution
  alias Journey.Execution.Value

  defp q_execution_ids_to_advance(execution_id, sweeper_period) do
    # Find all executions that have schedule_* computations that have "recently" come due.

    now = System.system_time(:second)
    cutoff_time = now - max(sweeper_period * 5, 60)

    from(e in q_executions(execution_id),
      join: c in assoc(e, :computations),
      join: v in Value,
      on:
        v.execution_id == e.id and
          v.node_name == c.node_name and
          v.node_type == c.computation_type and
          v.node_type in [:schedule_once, :schedule_recurring] and
          c.computation_type in [:schedule_once, :schedule_recurring],
      where:
        c.state == :success and
          not is_nil(v.set_time) and
          (v.node_value <= ^now or
             fragment("?::bigint", v.node_value) <= ^now) and
          v.set_time >= ^cutoff_time,
      distinct: true,
      select: e.id
    )
  end

  @doc false
  def sweep(execution_id, sweeper_period)
      when (is_nil(execution_id) or is_binary(execution_id)) and is_number(sweeper_period) do
    # Find and compute all un-computed computations that are downstream of computed schedule_once computations, that are due within the scheduled time window

    prefix = "[#{mf()}] [#{inspect(self())}]"
    Logger.debug("#{prefix}: starting #{execution_id}")

    q = q_execution_ids_to_advance(execution_id, sweeper_period)

    kicked_count =
      try do
        q
        |> Journey.Repo.all()
        |> Enum.map(fn swept_execution_id ->
          swept_execution_id
          |> Journey.load()
          |> Journey.Scheduler.advance()
        end)
        |> Enum.count()
      rescue
        e ->
          Logger.error("#{prefix}: error while sweeping: #{inspect(e)}")
          Logger.error("#{prefix}: query: #{inspect(Journey.Repo.to_sql(:all, q))}")
          reraise e, __STACKTRACE__
      end

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
