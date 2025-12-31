defmodule Journey.Scheduler.Heartbeat do
  @moduledoc """
  Sends periodic heartbeat updates for a running computation.

  Runs as a linked sibling to the worker process. Uses `trap_exit` to receive
  EXIT messages when the worker exits (normal or crash), allowing immediate
  cleanup without waiting for the next heartbeat interval.

  Exit conditions:
  - Worker exits → receives {:EXIT, pid, reason} → exits immediately
  - Worker crashes → heartbeat process receives EXIT, worker is already dead
  - Heartbeat process crashes → worker receives exit signal and dies (not trapping)
  - Computation state changes → `update_heartbeat` returns 0 rows → checks state
  - Deadline exceeded → marks as abandoned, exits non-normally to kill worker
  """

  require Logger
  import Ecto.Query

  alias Journey.Persistence.Schema.Execution.Computation

  # Buffer before enforcing deadline (allows sweep to act first)
  @deadline_buffer_seconds 10

  @doc """
  Runs the heartbeat loop for a computation.

  Spawned as a linked sibling to the worker - exits when:
  - Worker exits (link causes termination)
  - Computation state changes (update_heartbeat returns 0 rows)

  ## Arguments
    - computation_id: The ID of the computation to heartbeat
    - interval_seconds: How often to send heartbeats
    - timeout_seconds: The heartbeat timeout (used to calculate new deadline)
  """
  def run(execution_id, computation_id, node_name, interval_seconds, timeout_seconds) do
    Process.flag(:trap_exit, true)
    start_time = System.monotonic_time(:second)
    prefix = "Heartbeat run [#{execution_id}.#{computation_id}.#{node_name}]"
    Logger.info("#{prefix}: started")
    loop(execution_id, computation_id, node_name, interval_seconds, timeout_seconds)
    end_time = System.monotonic_time(:second)
    Logger.info("#{prefix}: exiting, after #{end_time - start_time} seconds")
  end

  defp loop(execution_id, computation_id, node_name, interval_seconds, timeout_seconds) do
    prefix = "Heartbeat loop [#{execution_id}.#{computation_id}.#{node_name}]"

    receive do
      {:EXIT, pid, _reason} ->
        Logger.info("#{prefix}: exiting, worker process #{inspect(pid)} exited")
        :ok
    after
      calculate_sleep_ms(interval_seconds) ->
        case do_heartbeat(execution_id, computation_id, node_name, timeout_seconds) do
          :continue ->
            loop(execution_id, computation_id, node_name, interval_seconds, timeout_seconds)

          :stop_normal ->
            :ok

          :kill_worker ->
            Logger.warning("#{prefix}: exiting the heartbeat process, also killing worker")
            exit(:computation_timeout)
        end
    end
  end

  defp calculate_sleep_ms(interval_seconds) do
    # Jitter is ±20% of the interval (range: 0.8 to 1.2)
    jitter_factor = 0.8 + 0.4 * :rand.uniform()
    trunc(interval_seconds * 1000 * jitter_factor)
  end

  defp do_heartbeat(execution_id, computation_id, node_name, timeout_seconds) do
    prefix = "Heartbeat pulse [#{execution_id}.#{computation_id}.#{node_name}]"

    try do
      case update_heartbeat(computation_id, timeout_seconds) do
        {0, _} ->
          # Query returned 0 rows - check why and decide action
          handle_update_failed(computation_id, prefix)

        {1, [new_deadline]} ->
          in_seconds = new_deadline - System.system_time(:second)
          Logger.info("#{prefix}: heartbeat recorded, new deadline: #{new_deadline} (in #{in_seconds} seconds)")
          :continue
      end
    rescue
      e ->
        Logger.warning("#{prefix}: heartbeat failed: #{inspect(e)}")
        :continue
    end
  end

  defp handle_update_failed(computation_id, prefix) do
    # Query the current state to decide action
    case Journey.Repo.get(Computation, computation_id) do
      nil ->
        Logger.warning("#{prefix}: computation not found, exiting")
        :stop_normal

      %{state: :completed} ->
        Logger.info("#{prefix}: computation completed, exiting normally")
        :stop_normal

      %{state: :error} ->
        Logger.info("#{prefix}: computation errored, exiting normally")
        :stop_normal

      %{state: :abandoned} ->
        # Sweep marked it abandoned - kill the worker
        Logger.info("#{prefix}: computation marked as abandoned by sweep")
        :kill_worker

      %{state: :computing} ->
        # Still computing but update failed - deadline must have exceeded
        Logger.warning("#{prefix}: deadline exceeded, marking as abandoned")
        mark_as_abandoned(computation_id)
        :kill_worker

      %{state: other_state} ->
        Logger.warning("#{prefix}: unexpected state #{inspect(other_state)}, exiting normally")
        :stop_normal
    end
  end

  defp update_heartbeat(computation_id, timeout_seconds) do
    now = System.system_time(:second)
    deadline_cutoff = now - @deadline_buffer_seconds

    from(c in Computation,
      where: c.id == ^computation_id and c.state == :computing and c.deadline > ^deadline_cutoff,
      select: c.heartbeat_deadline
    )
    |> Journey.Repo.update_all(
      set: [
        last_heartbeat_at: now,
        heartbeat_deadline: now + timeout_seconds
      ]
    )
  end

  defp mark_as_abandoned(computation_id) do
    now = System.system_time(:second)

    from(c in Computation,
      where: c.id == ^computation_id and c.state == :computing
    )
    |> Journey.Repo.update_all(
      set: [
        state: :abandoned,
        completion_time: now
      ]
    )
  end
end
