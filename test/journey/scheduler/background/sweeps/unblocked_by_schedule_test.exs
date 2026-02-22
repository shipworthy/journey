defmodule Journey.Scheduler.Background.Sweeps.UnblockedByScheduleTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Ecto.Query
  import Journey.Node
  import Journey.Helpers.Random

  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Scheduler.Background.Sweeps.UnblockedBySchedule

  describe "sweep/2" do
    test "detects due pulse when set_time is outside the 5x sweeper window" do
      # This tests the fix for the timing collision where tick_recurring nodes
      # with period >= 5 * sweeper_period were missed ~98% of the time.
      #
      # Scenario: tick computed 400s ago (set_time outside 300s window),
      # pulse became due 100s ago (node_value inside 300s window).
      # The old code filtered on set_time and would miss this.
      # The fix filters on node_value instead.

      graph =
        Journey.new_graph(
          "unblocked-schedule-test-#{random_string()}",
          "1.0",
          [
            tick_recurring(:my_tick, [], fn _ -> {:ok, System.system_time(:second) + 300} end)
          ]
        )

      execution = Journey.start_execution(graph)

      now = System.system_time(:second)
      sweeper_period = 60
      # set_time is outside the 5 * 60 = 300s window
      old_set_time = now - 400
      # node_value is due (in the past) but within the 300s window
      due_pulse_time = now - 100

      # Set computation to :success (simulating a completed tick)
      from(c in Computation,
        where: c.execution_id == ^execution.id and c.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [state: :success, completion_time: old_set_time])

      # Set value to a due pulse with an old set_time
      from(v in Value,
        where: v.execution_id == ^execution.id and v.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [node_value: due_pulse_time, set_time: old_set_time, ex_revision: 1])

      log =
        capture_log(fn ->
          UnblockedBySchedule.sweep(execution.id, sweeper_period)
        end)

      assert log =~ "kicked 1 execution"
    end

    test "does not detect pulse that is not yet due" do
      graph =
        Journey.new_graph(
          "unblocked-not-due-test-#{random_string()}",
          "1.0",
          [
            tick_recurring(:my_tick, [], fn _ -> {:ok, System.system_time(:second) + 300} end)
          ]
        )

      execution = Journey.start_execution(graph)

      now = System.system_time(:second)
      sweeper_period = 60
      future_pulse_time = now + 100

      from(c in Computation,
        where: c.execution_id == ^execution.id and c.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [state: :success, completion_time: now])

      from(v in Value,
        where: v.execution_id == ^execution.id and v.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [node_value: future_pulse_time, set_time: now, ex_revision: 1])

      log =
        capture_log(fn ->
          UnblockedBySchedule.sweep(execution.id, sweeper_period)
        end)

      assert log =~ "no recently due pulse value(s) found"
    end

    test "does not detect pulse that became due longer ago than the time window" do
      graph =
        Journey.new_graph(
          "unblocked-ancient-test-#{random_string()}",
          "1.0",
          [
            tick_recurring(:my_tick, [], fn _ -> {:ok, System.system_time(:second) + 300} end)
          ]
        )

      execution = Journey.start_execution(graph)

      now = System.system_time(:second)
      sweeper_period = 60
      # Pulse became due 400s ago — outside the 300s window
      ancient_pulse_time = now - 400

      from(c in Computation,
        where: c.execution_id == ^execution.id and c.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [state: :success, completion_time: now - 700])

      from(v in Value,
        where: v.execution_id == ^execution.id and v.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [node_value: ancient_pulse_time, set_time: now - 700, ex_revision: 1])

      log =
        capture_log(fn ->
          UnblockedBySchedule.sweep(execution.id, sweeper_period)
        end)

      assert log =~ "no recently due pulse value(s) found"
    end
  end
end
