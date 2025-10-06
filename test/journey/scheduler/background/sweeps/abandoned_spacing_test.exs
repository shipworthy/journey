defmodule Journey.Scheduler.Background.Sweeps.AbandonedSpacingTest do
  use ExUnit.Case, async: false
  import Ecto.Query

  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.Abandoned

  setup do
    # Clean slate - delete all sweep runs for abandoned sweep type
    Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :abandoned))
    :ok
  end

  describe "sweep/2 with SpacedOut timing" do
    test "first run succeeds and creates SweepRun record" do
      current_time = System.system_time(:second)

      {kicked_count, sweep_run_id} = Abandoned.sweep(nil, current_time)

      assert is_integer(kicked_count)
      assert is_binary(sweep_run_id)

      # Verify SweepRun record was created
      sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
      assert sweep_run.sweep_type == :abandoned
      assert sweep_run.started_at == current_time
      assert is_integer(sweep_run.completed_at)
      assert sweep_run.executions_processed == kicked_count
    end

    test "second run too soon returns skip with nil sweep_run_id" do
      current_time = System.system_time(:second)

      # First run
      {_kicked_count1, sweep_run_id1} = Abandoned.sweep(nil, current_time)
      assert is_binary(sweep_run_id1)

      # Second run 30 seconds later (less than min_seconds_between_runs threshold)
      too_soon_time = current_time + 30
      {kicked_count2, sweep_run_id2} = Abandoned.sweep(nil, too_soon_time)

      assert kicked_count2 == 0
      assert sweep_run_id2 == nil

      # Verify only one SweepRun record exists
      sweep_runs =
        from(sr in SweepRun, where: sr.sweep_type == :abandoned)
        |> Journey.Repo.all()

      assert length(sweep_runs) == 1
    end

    test "second run after min_seconds_between_runs succeeds" do
      # Get the configured min_seconds value
      min_seconds =
        Application.get_env(:journey, :abandoned_sweep, [])
        |> Keyword.get(:min_seconds_between_runs, 59)

      current_time = System.system_time(:second)

      # First run
      {_kicked_count1, sweep_run_id1} = Abandoned.sweep(nil, current_time)

      # Second run after min_seconds + 1 (to exceed the threshold)
      later_time = current_time + min_seconds + 1
      {_kicked_count2, sweep_run_id2} = Abandoned.sweep(nil, later_time)

      assert is_binary(sweep_run_id2)
      assert sweep_run_id2 != sweep_run_id1

      # Verify both records exist
      assert Journey.Repo.get!(SweepRun, sweep_run_id1)
      assert Journey.Repo.get!(SweepRun, sweep_run_id2)
    end

    test "second run exactly at threshold is skipped (exclusive boundary)" do
      # Get the configured min_seconds value
      min_seconds =
        Application.get_env(:journey, :abandoned_sweep, [])
        |> Keyword.get(:min_seconds_between_runs, 59)

      current_time = System.system_time(:second)

      # First run
      {_kicked_count1, _sweep_run_id1} = Abandoned.sweep(nil, current_time)

      # Second run exactly at min_seconds threshold (boundary is exclusive)
      {kicked_count2, sweep_run_id2} = Abandoned.sweep(nil, current_time + min_seconds)

      assert kicked_count2 == 0
      assert sweep_run_id2 == nil
    end

    test "concurrent runs - only one succeeds (advisory lock test)" do
      current_time = System.system_time(:second)

      # Spawn 5 concurrent tasks trying to run the same sweep
      tasks =
        for _i <- 1..5 do
          Task.async(fn ->
            Abandoned.sweep(nil, current_time)
          end)
        end

      # Wait for all tasks to complete
      results = Task.await_many(tasks, 10_000)

      # Count successful runs (non-nil sweep_run_id)
      successful_runs = Enum.count(results, fn {_count, sweep_run_id} -> sweep_run_id != nil end)
      skipped_runs = Enum.count(results, fn {_count, sweep_run_id} -> sweep_run_id == nil end)

      # Only one should succeed due to advisory lock
      assert successful_runs == 1
      assert skipped_runs == 4

      # Verify only one SweepRun record was created
      sweep_runs =
        from(sr in SweepRun, where: sr.sweep_type == :abandoned)
        |> Journey.Repo.all()

      assert length(sweep_runs) == 1
    end

    test "respects config override for min_seconds_between_runs" do
      # Override config to use 10 seconds instead of default 59
      original_config = Application.get_env(:journey, :abandoned_sweep, [])

      try do
        Application.put_env(:journey, :abandoned_sweep, min_seconds_between_runs: 10)

        current_time = System.system_time(:second)

        # First run
        {_kicked_count1, _sweep_run_id1} = Abandoned.sweep(nil, current_time)

        # Second run 11 seconds later should succeed (more than 10)
        later_time = current_time + 11
        {_kicked_count2, sweep_run_id2} = Abandoned.sweep(nil, later_time)

        assert is_binary(sweep_run_id2)

        # Verify both records exist
        sweep_runs =
          from(sr in SweepRun, where: sr.sweep_type == :abandoned)
          |> Journey.Repo.all()

        assert length(sweep_runs) == 2
      after
        # Restore original config
        Application.put_env(:journey, :abandoned_sweep, original_config)
      end
    end

    test "respects enabled config - disabled sweep returns {0, nil}" do
      original_config = Application.get_env(:journey, :abandoned_sweep, [])

      try do
        Application.put_env(:journey, :abandoned_sweep, enabled: false)

        current_time = System.system_time(:second)

        # Sweep should return {0, nil} immediately when disabled
        assert {0, nil} = Abandoned.sweep(nil, current_time)

        # Verify no SweepRun record was created
        sweep_runs =
          from(sr in SweepRun, where: sr.sweep_type == :abandoned)
          |> Journey.Repo.all()

        assert Enum.empty?(sweep_runs)
      after
        # Restore original config
        Application.put_env(:journey, :abandoned_sweep, original_config)
      end
    end

    test "respects enabled config - enabled sweep runs normally" do
      original_config = Application.get_env(:journey, :abandoned_sweep, [])

      try do
        Application.put_env(:journey, :abandoned_sweep, enabled: true)

        current_time = System.system_time(:second)

        # Sweep should run normally when enabled
        {kicked_count, sweep_run_id} = Abandoned.sweep(nil, current_time)

        assert is_integer(kicked_count)
        assert is_binary(sweep_run_id)

        # Verify SweepRun record was created
        sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
        assert sweep_run.sweep_type == :abandoned
      after
        # Restore original config
        Application.put_env(:journey, :abandoned_sweep, original_config)
      end
    end
  end
end
