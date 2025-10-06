defmodule Journey.Scheduler.Background.Sweeps.Helpers.ThrottleTest do
  use ExUnit.Case, async: true
  import Ecto.Query

  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.Helpers.Throttle

  setup do
    # Clean slate - delete all sweep runs for test sweep types
    Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :test_sweep))
    Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :test_sweep2))
    :ok
  end

  describe "attempt_to_start_sweep_run/3" do
    test "first run succeeds and creates SweepRun record" do
      current_time = System.system_time(:second)
      min_seconds = 100

      {:ok, sweep_run_id} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)

      assert is_binary(sweep_run_id)

      # Verify SweepRun record was created
      sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
      assert sweep_run.sweep_type == :test_sweep
      assert sweep_run.started_at == current_time
      assert sweep_run.completed_at == nil
      assert sweep_run.executions_processed == 0
    end

    test "second run too soon returns skip" do
      current_time = System.system_time(:second)
      min_seconds = 100

      # First run
      {:ok, _sweep_run_id1} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)

      # Second run 50 seconds later (less than 100)
      too_soon_time = current_time + 50
      {:skip, reason} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, too_soon_time)
      assert reason == "only 50 seconds since last run (min: 100)"
    end

    test "second run after enough time succeeds" do
      current_time = System.system_time(:second)
      min_seconds = 100

      # First run
      {:ok, sweep_run_id1} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)

      # Second run 150 seconds later (more than 100)
      later_time = current_time + 150
      {:ok, sweep_run_id2} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, later_time)

      assert is_binary(sweep_run_id2)
      assert sweep_run_id2 != sweep_run_id1

      # Verify both records exist
      assert Journey.Repo.get!(SweepRun, sweep_run_id1)
      assert Journey.Repo.get!(SweepRun, sweep_run_id2)
    end

    test "second run exactly at threshold is skipped (exclusive boundary)" do
      current_time = System.system_time(:second)
      min_seconds = 100

      # First run
      {:ok, _sweep_run_id1} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)

      # Second run exactly 100 seconds later (boundary is exclusive)
      {:skip, reason} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time + 100)
      assert reason == "only 100 seconds since last run (min: 100)"
    end

    test "second run just after threshold succeeds" do
      current_time = System.system_time(:second)
      min_seconds = 100

      # First run
      {:ok, sweep_run_id1} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)

      # Second run 101 seconds later (just after threshold)
      {:ok, sweep_run_id2} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time + 101)

      assert is_binary(sweep_run_id2)
      assert sweep_run_id2 != sweep_run_id1
    end

    test "concurrent runs - only one succeeds (advisory lock test)" do
      current_time = System.system_time(:second)
      min_seconds = 100

      # Spawn 5 concurrent tasks trying to start the same sweep
      tasks =
        for _i <- 1..5 do
          Task.async(fn ->
            Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)
          end)
        end

      # Wait for all tasks to complete
      results = Task.await_many(tasks, 10_000)

      # Count successful runs
      successful_runs = Enum.count(results, fn result -> match?({:ok, _}, result) end)
      skipped_runs = Enum.count(results, fn result -> match?({:skip, _}, result) end)

      # Only one should succeed due to advisory lock
      assert successful_runs == 1
      assert skipped_runs == 4

      # Verify only one SweepRun record was created
      sweep_runs =
        from(sr in SweepRun, where: sr.sweep_type == :test_sweep)
        |> Journey.Repo.all()

      assert length(sweep_runs) == 1
    end

    test "different sweep types are independent" do
      current_time = System.system_time(:second)
      min_seconds = 100

      # Start both sweep types concurrently
      # Both should succeed because they use different lock IDs
      {:ok, sweep_run_id1} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)
      {:ok, sweep_run_id2} = Throttle.attempt_to_start_sweep_run(:test_sweep2, min_seconds, current_time)

      assert is_binary(sweep_run_id1)
      assert is_binary(sweep_run_id2)
      assert sweep_run_id1 != sweep_run_id2

      # Verify both records exist
      sweep_run1 = Journey.Repo.get!(SweepRun, sweep_run_id1)
      sweep_run2 = Journey.Repo.get!(SweepRun, sweep_run_id2)

      assert sweep_run1.sweep_type == :test_sweep
      assert sweep_run2.sweep_type == :test_sweep2
    end
  end

  describe "complete_started_sweep_run/3" do
    test "updates SweepRun record with completion info" do
      current_time = System.system_time(:second)
      min_seconds = 100

      # Start a sweep run
      {:ok, sweep_run_id} = Throttle.attempt_to_start_sweep_run(:test_sweep, min_seconds, current_time)

      # Verify initial state
      sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
      assert sweep_run.completed_at == nil
      assert sweep_run.executions_processed == 0

      # Complete the sweep
      completion_time = current_time + 50
      executions_processed = 42

      updated_sweep_run = Throttle.complete_started_sweep_run(sweep_run_id, executions_processed, completion_time)

      assert updated_sweep_run.id == sweep_run_id

      # Verify completion info was updated
      assert updated_sweep_run.completed_at == completion_time
      assert updated_sweep_run.executions_processed == executions_processed
      assert updated_sweep_run.sweep_type == :test_sweep
      assert updated_sweep_run.started_at == current_time

      # Verify persistence
      persisted_sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
      assert persisted_sweep_run.completed_at == completion_time
      assert persisted_sweep_run.executions_processed == executions_processed
    end
  end
end
