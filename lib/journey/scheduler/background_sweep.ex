defmodule Journey.Scheduler.BackgroundSweep do
  @moduledoc false

  require Logger
  import Ecto.Query
  alias Journey.Execution.Computation
  alias Journey.Execution.Value

  import Journey.Helpers.Log

  @mode if Mix.env() != :test, do: :auto, else: :manual

  def child_spec(_arg) do
    Periodic.child_spec(
      id: __MODULE__,
      mode: @mode,
      initial_delay: :timer.seconds(:rand.uniform(20) + 5),
      run: &run/0,
      every: :timer.seconds(5),
      delay_mode: :shifted
    )
  end

  def run() do
    # TODO: replace this with the logic that executes on the same period,
    # regardless of # of replicas.
    Process.flag(:trap_exit, true)
    prefix = "#{mf()}[#{inspect(self())}]"
    Logger.debug("#{prefix}: starting")

    try do
      Logger.debug("#{prefix}: performing sweep")
      find_and_kickoff_abandoned_computations(nil)
      find_and_kick_recently_due_pulse_values(nil)
      Logger.debug("#{prefix}: sweep complete")
    catch
      exception ->
        Logger.error("#{prefix}: #{inspect(exception)}")
    end

    Logger.debug("#{prefix}: done")
  end

  def find_and_kickoff_abandoned_computations(execution_id) do
    sweep_abandoned_computations(execution_id)
    |> Enum.map(fn %{execution_id: execution_id} -> execution_id end)
    |> Enum.uniq()
    |> Enum.map(fn swept_execution_id -> kick(swept_execution_id) end)
  end

  defp kick(execution_id) do
    prefix = "[#{execution_id}] [#{mf()}] [#{inspect(self())}]"
    Logger.info("#{prefix}: processing execution")

    execution_id
    |> Journey.load()
    |> Journey.Scheduler.advance()
  end

  def sweep_abandoned_computations(execution_id) do
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

  def find_and_kick_recently_due_pulse_values(execution_id) do
    prefix = "[#{mf()}] [#{inspect(self())}]"
    Logger.debug("#{prefix}: starting")

    now = System.system_time(:second)
    yesterday = now - 24 * 60 * 60

    q = from_values(execution_id)

    kicked_count =
      from(v in q,
        # TODO: scalability. Find a better way to limit the set than having a lower time bound.
        where:
          v.node_type == ^:pulse_once and
            fragment("CAST(?->>'v' AS INTEGER) < ?", v.node_value, ^now) and
            fragment("CAST(?->>'v' AS INTEGER) > ?", v.node_value, ^yesterday)
      )
      |> Journey.Repo.all()
      |> Enum.map(fn %{execution_id: execution_id} -> execution_id end)
      |> Enum.uniq()
      |> Enum.map(fn swept_execution_id -> kick(swept_execution_id) end)
      |> Enum.count()

    if kicked_count == 0 do
      Logger.debug("#{prefix}: no recently due pulse value(s) found")
    else
      Logger.info("#{prefix}: completed. kicked #{kicked_count} execution(s)")
    end
  end

  defp from_values(nil) do
    from(v in Value)
  end

  defp from_values(execution_id) do
    from(v in Value, where: v.execution_id == ^execution_id)
  end

  defp from_computations(nil) do
    from(c in Computation)
  end

  defp from_computations(execution_id) do
    from(c in Computation, where: c.execution_id == ^execution_id)
  end
end
