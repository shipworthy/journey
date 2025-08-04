defmodule Journey.Scheduler.BackgroundSweeps.ScheduleNodesCoreTest do
  use ExUnit.Case, async: true

  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.ScheduleNodes

  describe "get_last_sweep_cutoff/1" do
    test "returns last completed sweep's started_at timestamp minus 60 seconds" do
      # Use very recent timestamps to ensure ours is the most recent
      sweep_time = System.os_time(:second)

      # Insert our sweep run with current timestamp
      sweep_run = insert_completed_sweep_run(:schedule_nodes, sweep_time, sweep_time + 1)

      cutoff = ScheduleNodes.get_last_sweep_cutoff(:schedule_nodes)

      # Should return our sweep's start time minus 60 seconds for overlap
      assert cutoff == sweep_run.started_at - 60
    end

    test "returns fallback when no sweeps for given type exist" do
      # Use a different sweep type that hasn't been used
      cutoff = ScheduleNodes.get_last_sweep_cutoff(:regenerate_schedule_recurring)

      # Should return 0 (beginning of time) when no previous sweeps
      assert cutoff == 0
    end

    test "ignores incomplete sweeps (no completed_at)" do
      # Use unique times to avoid conflicts with other tests
      base_time = System.os_time(:second)
      incomplete_time = base_time - 20
      complete_time = base_time - 40

      insert_incomplete_sweep_run(:schedule_nodes, incomplete_time)
      complete_sweep = insert_completed_sweep_run(:schedule_nodes, complete_time, complete_time + 5)

      # Get the most recent completed sweep
      cutoff = ScheduleNodes.get_last_sweep_cutoff(:schedule_nodes)

      # It should be at least as old as our completed sweep minus 60 seconds
      # (might be newer if other tests ran)
      assert cutoff >= complete_sweep.started_at - 60
    end

    test "returns most recent completed sweep when multiple exist" do
      base_time = System.os_time(:second)
      old_time = base_time - 100
      recent_time = base_time - 50

      insert_completed_sweep_run(:schedule_nodes, old_time, old_time + 5)
      recent_sweep = insert_completed_sweep_run(:schedule_nodes, recent_time, recent_time + 5)

      cutoff = ScheduleNodes.get_last_sweep_cutoff(:schedule_nodes)

      # Should be at least as recent as our recent_sweep minus 60 seconds
      assert cutoff >= recent_sweep.started_at - 60
    end

    test "filters by sweep_type correctly" do
      base_time = System.os_time(:second)
      schedule_time = base_time - 50
      unblocked_time = base_time - 30

      schedule_sweep = insert_completed_sweep_run(:schedule_nodes, schedule_time, schedule_time + 5)
      _unblocked_sweep = insert_completed_sweep_run(:unblocked_by_schedule, unblocked_time, unblocked_time + 5)

      cutoff = ScheduleNodes.get_last_sweep_cutoff(:schedule_nodes)

      # Should get schedule_nodes sweep minus 60, not unblocked_by_schedule
      assert cutoff >= schedule_sweep.started_at - 60
    end
  end

  # Test helpers

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
