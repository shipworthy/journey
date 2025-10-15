defmodule Journey.Scheduler.Background.Sweeps.UnblockedBySchedule do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Scheduler.Background.Sweeps.Helpers
  alias Journey.Persistence.Schema.Execution.Value

  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp q_execution_ids_to_advance(execution_id, sweeper_period_seconds) do
    # Find all executions that have schedule_* computations that have "recently" come due.

    now = System.system_time(:second)
    time_window_seconds = 5 * sweeper_period_seconds
    cutoff_time = now - time_window_seconds

    all_graphs = get_registered_graphs()

    from(e in executions_for_graphs(execution_id, all_graphs),
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
          fragment("?::bigint", v.node_value) > 0 and
          fragment("?::bigint", v.node_value) <= ^now and
          v.set_time >= ^cutoff_time,
      distinct: true,
      select: e.id
    )
  end

  defp get_registered_graphs do
    Journey.Graph.Catalog.list()
    |> Enum.map(fn g -> {g.name, g.version} end)
  end

  @doc false
  def sweep(execution_id, sweeper_period)
      when (is_nil(execution_id) or is_binary(execution_id)) and is_number(sweeper_period) do
    Logger.info("starting #{execution_id}")

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
          Logger.error("error while sweeping: #{inspect(e)}")
          Logger.error("query: #{inspect(Journey.Repo.to_sql(:all, q))}")
          reraise e, __STACKTRACE__
      end

    if kicked_count == 0 do
      Logger.info("no recently due pulse value(s) found")
    else
      Logger.info("completed. kicked #{kicked_count} execution(s)")
    end
  end
end
