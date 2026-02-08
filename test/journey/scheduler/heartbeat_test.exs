defmodule Journey.Scheduler.HeartbeatTest do
  use ExUnit.Case, async: true
  import Ecto.Query
  import Journey.Node
  import Journey.Helpers.Random, only: [random_string: 0]

  alias Journey.Persistence.Schema.Execution.Computation

  describe "graph validation" do
    test "rejects heartbeat interval < 30s" do
      assert_raise RuntimeError, ~r/heartbeat_interval_seconds must be >= 30 seconds/, fn ->
        Journey.new_graph("bad_heartbeat_1_#{random_string()}", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end, heartbeat_interval_seconds: 29)
        ])
      end
    end

    test "rejects interval > timeout / 2" do
      assert_raise RuntimeError, ~r/must be <= half of heartbeat_timeout_seconds/, fn ->
        Journey.new_graph("bad_heartbeat_2_#{random_string()}", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end,
            heartbeat_interval_seconds: 40,
            heartbeat_timeout_seconds: 70
          )
        ])
      end
    end

    test "accepts valid configuration" do
      graph =
        Journey.new_graph("good_heartbeat_#{random_string()}", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end,
            heartbeat_interval_seconds: 30,
            heartbeat_timeout_seconds: 60
          )
        ])

      assert graph
    end
  end

  describe "heartbeat initialization" do
    test "updates heartbeat fields on start" do
      timeout = 100

      graph =
        Journey.new_graph("heartbeat_init_#{random_string()}", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end,
            heartbeat_interval_seconds: 30,
            heartbeat_timeout_seconds: timeout
          )
        ])

      execution = Journey.start_execution(graph)
      {:ok, 1, _} = Journey.get(execution, :step1, wait: :any, timeout: 5_000)

      comp = get_latest_computation(execution.id, :step1)

      # heartbeat_deadline is initialized; last_heartbeat_at remains nil until first heartbeat
      assert comp.last_heartbeat_at == nil
      assert comp.heartbeat_deadline == comp.start_time + timeout
    end
  end

  describe "heartbeat during execution" do
    @tag timeout: 100_000
    test "heartbeat is updated during long-running computation" do
      interval = 30
      timeout = 70

      graph =
        Journey.new_graph("heartbeat_during_execution_#{random_string()}", "1", [
          compute(
            :slow_step_75_seconds,
            [],
            fn _ ->
              # Sleep past two heartbeat intervals (2 × 30s + jitter buffer)
              Process.sleep(75_000)
              {:ok, "done"}
            end,
            heartbeat_interval_seconds: interval,
            heartbeat_timeout_seconds: timeout
          )
        ])

      execution = Journey.start_execution(graph)
      {:ok, "done", _} = Journey.get(execution, :slow_step_75_seconds, wait: :any, timeout: 90_000)

      comp = get_latest_computation(execution.id, :slow_step_75_seconds)

      # Verify at least 2 heartbeats fired:
      # First heartbeat fires in 24-36s window (30s ± 20% jitter)
      # If last_heartbeat_at > 36s, a second heartbeat must have occurred
      assert comp.last_heartbeat_at > comp.start_time + trunc(interval * 1.2)

      # Verify deadline was extended correctly on last heartbeat
      assert comp.heartbeat_deadline == comp.last_heartbeat_at + timeout
    end
  end

  describe "deadline enforcement" do
    @tag timeout: 120_000
    test "kills worker when deadline exceeded" do
      # Use short abandon_after_seconds (35s) with worker that sleeps forever
      # First heartbeat at ~30s succeeds (deadline 35 > 30-10=20)
      # Second heartbeat at ~60s fails (deadline 35 < 60-10=50)
      # Heartbeat marks as abandoned and kills worker

      graph =
        Journey.new_graph("deadline_enforcement_#{random_string()}", "1", [
          compute(
            :runaway_worker,
            [],
            fn _ ->
              # Sleep "forever" - heartbeat should kill us before this completes
              Process.sleep(300_000)
              {:ok, "should not reach this"}
            end,
            abandon_after_seconds: 35,
            heartbeat_interval_seconds: 30,
            heartbeat_timeout_seconds: 70
          )
        ])

      start_time = System.monotonic_time(:second)
      execution = Journey.start_execution(graph)

      # Wait for computation to be killed (should happen around 50-70s, not 300s)
      # Poll until abandoned or timeout
      comp = wait_for_state(execution.id, :runaway_worker, :abandoned, 90_000)

      end_time = System.monotonic_time(:second)
      elapsed = end_time - start_time

      # Verify it was marked as abandoned
      assert comp.state == :abandoned

      # Verify it happened much faster than the 300s sleep
      # Should be around 50-70s (second heartbeat detects deadline exceeded)
      assert elapsed < 90, "Expected worker to be killed within 90s, took #{elapsed}s"
      assert elapsed > 40, "Expected worker to run at least 40s before being killed, took #{elapsed}s"
    end
  end

  defp wait_for_state(execution_id, node_name, expected_state, timeout_ms) do
    node_name_str = Atom.to_string(node_name)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    wait_loop(execution_id, node_name_str, expected_state, deadline)
  end

  defp wait_loop(execution_id, node_name_str, expected_state, deadline) do
    comp =
      from(c in Computation,
        where: c.execution_id == ^execution_id and c.node_name == ^node_name_str,
        order_by: [desc: c.inserted_at],
        limit: 1
      )
      |> Journey.Repo.one()

    cond do
      comp && comp.state == expected_state ->
        comp

      System.monotonic_time(:millisecond) > deadline ->
        flunk("Timed out waiting for state #{expected_state}, current state: #{comp && comp.state}")

      true ->
        Process.sleep(1_000)
        wait_loop(execution_id, node_name_str, expected_state, deadline)
    end
  end

  defp get_latest_computation(execution_id, node_name) do
    node_name_str = Atom.to_string(node_name)

    from(c in Computation,
      where: c.execution_id == ^execution_id and c.node_name == ^node_name_str,
      order_by: [desc: c.inserted_at],
      limit: 1
    )
    |> Journey.Repo.one!()
  end
end
