defmodule Journey.Scheduler.HeartbeatTest do
  use ExUnit.Case, async: true
  import Ecto.Query
  import Journey.Node

  alias Journey.Persistence.Schema.Execution.Computation

  describe "graph validation" do
    test "rejects heartbeat interval < 30s" do
      assert_raise RuntimeError, ~r/heartbeat_interval_seconds be >= 30 seconds/, fn ->
        Journey.new_graph("bad_heartbeat_1", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end, heartbeat_interval_seconds: 29)
        ])
      end
    end

    test "rejects interval > timeout / 2" do
      assert_raise RuntimeError, ~r/must be <= half of heartbeat_timeout_seconds/, fn ->
        Journey.new_graph("bad_heartbeat_2", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end,
            heartbeat_interval_seconds: 40,
            heartbeat_timeout_seconds: 70
          )
        ])
      end
    end

    test "accepts valid configuration" do
      graph =
        Journey.new_graph("good_heartbeat", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end,
            heartbeat_interval_seconds: 30,
            heartbeat_timeout_seconds: 60
          )
        ])

      assert graph
    end
  end

  describe "watchdog initialization" do
    test "updates heartbeat fields on start" do
      graph =
        Journey.new_graph("heartbeat_init", "1", [
          compute(:step1, [], fn _ -> {:ok, 1} end,
            heartbeat_interval_seconds: 30,
            heartbeat_timeout_seconds: 100
          )
        ])

      execution = Journey.start_execution(graph)

      # Wait for the computation to be created and in :computing state
      comp = wait_for_computation(execution.id, :step1, :computing)

      assert comp.last_heartbeat_at != nil
      assert comp.heartbeat_deadline != nil
      assert comp.heartbeat_deadline == comp.start_time + 100
    end
  end

  describe "watchdog heartbeat during execution" do
    test "heartbeat is updated during long-running computation" do
      # Use minimum allowed values: interval 30, timeout 70 (30 <= 70/2 = 35)
      graph =
        Journey.new_graph("heartbeat_during_execution", "1", [
          compute(
            :slow_step,
            [],
            fn _ ->
              # Sleep past one heartbeat interval (30s + jitter buffer)
              Process.sleep(40_000)
              {:ok, "done"}
            end,
            heartbeat_interval_seconds: 30,
            heartbeat_timeout_seconds: 70
          )
        ])

      execution = Journey.start_execution(graph)

      # Wait for computation to start and capture initial values
      comp_before = wait_for_computation(execution.id, :slow_step, :computing)
      initial_heartbeat_at = comp_before.last_heartbeat_at
      initial_deadline = comp_before.heartbeat_deadline

      # Wait for computation to complete
      {:ok, "done", _} = Journey.get(execution, :slow_step, wait: :any, timeout: 50_000)

      # Query the final computation record
      comp_after = get_latest_computation(execution.id, :slow_step)

      # Heartbeat should have fired at least once during the 40s computation
      assert comp_after.last_heartbeat_at > initial_heartbeat_at,
             "Expected heartbeat to update from #{initial_heartbeat_at} but got #{comp_after.last_heartbeat_at}"

      # Deadline should have been extended
      assert comp_after.heartbeat_deadline > initial_deadline,
             "Expected deadline to extend from #{initial_deadline} but got #{comp_after.heartbeat_deadline}"
    end
  end

  describe "Watchdog module" do
    test "updates heartbeat fields in database" do
      # Create a graph/execution just to get a computation in :computing state
      graph =
        Journey.new_graph("watchdog_unit_test", "1", [
          compute(
            :test_step,
            [],
            fn _ ->
              # This will block forever, we'll kill it manually
              Process.sleep(:infinity)
              {:ok, "never"}
            end,
            heartbeat_interval_seconds: 30,
            heartbeat_timeout_seconds: 70
          )
        ])

      execution = Journey.start_execution(graph)

      # Wait for computation to enter :computing state
      comp = wait_for_computation(execution.id, :test_step, :computing)
      initial_heartbeat_at = comp.last_heartbeat_at
      initial_deadline = comp.heartbeat_deadline

      # Spawn our own watchdog with a short interval (bypasses graph validation)
      # The real watchdog is already running, but we can spawn another one
      # Use timeout >= 70 so the deadline increases (original uses 70s timeout)
      test_watchdog_pid =
        spawn(fn ->
          # Use 1s interval for fast testing, 100s timeout so deadline increases
          Journey.Scheduler.Watchdog.run(comp.id, 1, 100)
        end)

      # Wait for a couple of heartbeat cycles
      Process.sleep(2_500)

      # Query updated computation
      updated_comp = Journey.Repo.get!(Computation, comp.id)

      # Verify heartbeat was updated
      assert updated_comp.last_heartbeat_at > initial_heartbeat_at,
             "Expected last_heartbeat_at to increase from #{initial_heartbeat_at} but got #{updated_comp.last_heartbeat_at}"

      assert updated_comp.heartbeat_deadline > initial_deadline,
             "Expected heartbeat_deadline to increase from #{initial_deadline} but got #{updated_comp.heartbeat_deadline}"

      # Cleanup: kill the test watchdog
      Process.exit(test_watchdog_pid, :kill)

      # The original computation Task is still running with Process.sleep(:infinity)
      # It will be cleaned up when the test process exits (linked processes die)
    end
  end

  defp wait_for_computation(execution_id, node_name, expected_state) do
    node_name_str = Atom.to_string(node_name)

    query =
      from(c in Computation,
        where: c.execution_id == ^execution_id and c.node_name == ^node_name_str and c.state == ^expected_state
      )

    import WaitForIt

    wait(Journey.Repo.one(query), timeout: 5000, frequency: 100) ||
      flunk("Timed out waiting for computation #{node_name} in state #{expected_state}")
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
