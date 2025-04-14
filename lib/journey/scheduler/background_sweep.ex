defmodule Journey.Scheduler.BackgroundSweep do
  @moduledoc false

  require Logger

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
      Logger.info("#{prefix}: performing sweep")
      find_and_kickoff_abandoned_computations()
      Logger.info("#{prefix}: sweep complete")
    catch
      exception ->
        Logger.error("#{prefix}: #{inspect(exception)}")
    end

    Logger.debug("#{prefix}: done")
  end

  def find_and_kickoff_abandoned_computations() do
    Journey.Scheduler.Operations.sweep_abandoned_computations(nil)
    |> Enum.map(fn %{execution_id: execution_id} -> execution_id end)
    |> Enum.uniq()
    |> Enum.map(fn execution_id -> kick(execution_id) end)
  end

  defp kick(execution_id) do
    prefix = "[#{execution_id}] [#{mf()}] [#{inspect(self())}]"
    Logger.info("#{prefix}: processing execution")

    execution_id
    |> Journey.load()
    |> Journey.Scheduler.Operations.advance()
  end
end
