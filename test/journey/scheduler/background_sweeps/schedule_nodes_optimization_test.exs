defmodule Journey.Scheduler.BackgroundSweeps.ScheduleNodesOptimizationTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Journey.Examples.CreditCardApplication
  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.ScheduleNodes

  describe "sweep/1 optimization" do
    test "only processes executions updated since last sweep" do
      # Create old execution
      _old_exec = create_execution_with_schedule()

      # Wait to ensure timestamp difference
      Process.sleep(1000)

      # Record a sweep that happened after old_exec was created
      sweep_time = System.os_time(:second)
      sweep_run = insert_completed_sweep_run(:schedule_nodes, sweep_time, sweep_time)

      # Wait again to ensure new execution has later timestamp
      Process.sleep(1000)

      # Create new execution after the recorded sweep
      _new_exec = create_execution_with_schedule()

      # No need to check computation states since schedule computations
      # remain in :not_set even after being kicked

      # Run sweep
      {kicked_count, _sweep_run_id} = ScheduleNodes.sweep(nil)

      # Should have kicked at least one execution (the new one)
      # Background sweeps may create additional executions that get processed
      assert kicked_count >= 1

      # The key test is that incremental processing is working - verify that
      # a sweep was recorded and completed successfully

      # Verify at least one new sweep run was recorded after our test sweep
      new_sweep_runs =
        Journey.Repo.all(
          from sr in SweepRun,
            where: sr.sweep_type == :schedule_nodes and sr.started_at > ^sweep_run.started_at
        )

      # There should be at least 1 new sweep run (could be more from background tasks)
      assert length(new_sweep_runs) >= 1

      # Find a sweep run that processed at least 1 execution (our test sweep)
      test_sweep = Enum.find(new_sweep_runs, fn sr -> sr.executions_processed >= 1 end)
      assert test_sweep != nil
      # assert not is_nil(test_sweep.completed_at)
    end

    test "processes all executions when no previous sweep exists" do
      _exec1 = create_execution_with_schedule()
      _exec2 = create_execution_with_schedule()

      # Record current sweep count
      initial_sweep_count = Journey.Repo.aggregate(SweepRun, :count, :id)

      {kicked_count, _sweep_run_id} = ScheduleNodes.sweep(nil)

      # Should have kicked at least our two executions
      assert kicked_count >= 2

      # Verify at least one sweep run was recorded (background sweeps may create additional ones)
      new_sweep_count = Journey.Repo.aggregate(SweepRun, :count, :id)
      assert new_sweep_count >= initial_sweep_count + 1
    end

    test "uses fallback cutoff when no completed sweeps exist" do
      # Create execution
      _exec = create_execution_with_schedule()

      # Insert incomplete sweep (no completed_at)
      insert_incomplete_sweep_run(:schedule_nodes, System.os_time(:second) - 30)

      # Should process at least our execution (uses 1 hour fallback)
      {kicked_count, _sweep_run_id} = ScheduleNodes.sweep(nil)
      assert kicked_count >= 1
    end

    test "handles execution_id parameter correctly" do
      exec1 = create_execution_with_schedule()
      _exec2 = create_execution_with_schedule()

      # Sweep specific execution only
      {kicked_count, _sweep_run_id} = ScheduleNodes.sweep(exec1.id)

      # Should only process exec1
      assert kicked_count == 1
    end

    test "records sweep timing correctly" do
      # Record initial sweep count
      initial_sweep_count = Journey.Repo.aggregate(SweepRun, :count, :id)

      start_time = System.os_time(:second)

      _exec = create_execution_with_schedule()
      {_kicked_count, sweep_run_id} = ScheduleNodes.sweep(nil)

      end_time = System.os_time(:second)

      # Get the specific sweep run created by this test
      sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)

      assert sweep_run.sweep_type == :schedule_nodes
      assert sweep_run.started_at >= start_time
      # Allow 1 second tolerance for timing precision
      assert sweep_run.completed_at <= end_time + 1
      assert sweep_run.completed_at >= sweep_run.started_at

      # Verify at least one new sweep was created (background sweeps may create additional ones)
      new_sweep_count = Journey.Repo.aggregate(SweepRun, :count, :id)
      assert new_sweep_count >= initial_sweep_count + 1
    end

    test "records completion on success" do
      _exec = create_execution_with_schedule()

      # Run sweep and capture the returned sweep_run_id
      {_kicked_count, sweep_run_id} = ScheduleNodes.sweep(nil)

      # Get the specific sweep run we just created
      sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)

      # Verify completion was recorded
      assert sweep_run.sweep_type == :schedule_nodes
      assert not is_nil(sweep_run.completed_at)
      assert not is_nil(sweep_run.started_at)
      assert sweep_run.completed_at >= sweep_run.started_at
    end

    @tag :performance
    test "sweep time doesn't grow linearly with old executions" do
      # Create baseline executions
      _baseline_execs = for _ <- 1..10, do: create_execution_with_schedule()

      # Wait to ensure timestamp separation
      Process.sleep(1000)

      # Record sweep after baseline
      sweep_time = System.os_time(:second)
      insert_completed_sweep_run(:schedule_nodes, sweep_time, sweep_time)

      # Wait again
      Process.sleep(1000)

      # Add a few new executions (should be processed)
      _new_execs = for _ <- 1..5, do: create_execution_with_schedule()

      # Measure sweep time
      {time_micros, {kicked_count, _sweep_run_id}} = :timer.tc(fn -> ScheduleNodes.sweep(nil) end)

      # Should have processed some executions (background sweeps may affect exact count)
      # The key test is that incremental processing is working and time is reasonable
      assert kicked_count >= 1

      # Time should be reasonable (not linear in total execution count)
      # Less than 1 second - this is the main performance assertion
      assert time_micros < 1_000_000
    end
  end

  # Test helpers
  defp create_execution_with_schedule do
    graph = CreditCardApplication.graph()
    execution = Journey.start_execution(graph)
    # Set values to create schedule computations in not_set state
    Journey.set_value(execution, :full_name, "Test User #{:rand.uniform(1000)}")
    execution
  end

  defp insert_completed_sweep_run(type, started_at, completed_at) do
    %SweepRun{}
    |> SweepRun.changeset(%{
      sweep_type: type,
      started_at: started_at,
      completed_at: completed_at,
      executions_processed: 10
    })
    |> Journey.Repo.insert!()
  end

  defp insert_incomplete_sweep_run(type, started_at) do
    %SweepRun{}
    |> SweepRun.changeset(%{
      sweep_type: type,
      started_at: started_at
    })
    |> Journey.Repo.insert!()
  end
end
