defmodule Journey.Scheduler.Watchdog do
  @moduledoc """
  Sends periodic heartbeat updates for a running computation.
  Linked to the worker process - dies when worker dies.

  The watchdog runs as a simple loop (not a GenServer) since it's fire-and-forget
  and linked to the worker Task. When the worker completes or crashes, the
  linked watchdog also terminates.
  """

  require Logger
  import Ecto.Query

  alias Journey.Persistence.Schema.Execution.Computation

  @doc """
  Runs the heartbeat loop for a computation.

  Args:
    - computation_id: The ID of the computation to heartbeat
    - interval_seconds: How often to send heartbeats
    - timeout_seconds: The heartbeat timeout (used to calculate new deadline)
  """
  def run(computation_id, interval_seconds, timeout_seconds) do
    start_time = System.monotonic_time(:second)
    Logger.info("Heartbeats for computation #{computation_id} started")
    loop(computation_id, interval_seconds, timeout_seconds)
    end_time = System.monotonic_time(:second)
    Logger.info("Heartbeat for computation #{computation_id} completed, in #{end_time - start_time} seconds")
  end

  defp loop(computation_id, interval_seconds, timeout_seconds) do
    # Jitter is ±20% of the interval
    # range: 0.8 to 1.2
    jitter_factor = 0.8 + 0.4 * :rand.uniform()
    sleep_ms = trunc(interval_seconds * 1000 * jitter_factor)
    Process.sleep(sleep_ms)

    continue? =
      try do
        case update_heartbeat(computation_id, timeout_seconds) do
          {0, _} ->
            # Computation is no longer :computing - exit the watchdog.
            false

          {_count, _} ->
            true
        end
      rescue
        e ->
          Logger.warning("Heartbeat update failed for computation #{computation_id}: #{inspect(e)}")
          # Continue trying on transient errors
          true
      end

    if continue? do
      loop(computation_id, interval_seconds, timeout_seconds)
    end
  end

  defp update_heartbeat(computation_id, timeout_seconds) do
    now = System.system_time(:second)

    from(c in Computation,
      where: c.id == ^computation_id and c.state == :computing
    )
    |> Journey.Repo.update_all(
      set: [
        last_heartbeat_at: now,
        heartbeat_deadline: now + timeout_seconds
      ]
    )
  end
end
