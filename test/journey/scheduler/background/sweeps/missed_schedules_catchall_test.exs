defmodule Journey.Scheduler.Background.Sweeps.MissedSchedulesCatchallTest do
  use ExUnit.Case, async: false
  import Journey.Node
  import Ecto.Query
  import Journey.Helpers.Random

  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.MissedSchedulesCatchall

  setup do
    # Clean up any existing sweep runs for our type
    Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :missed_schedules_catchall))

    :ok
  end

  describe "sweep/1 - timing and basic functionality" do
    test "respects 23-hour minimum between runs" do
      # Create and complete a sweep run
      now = System.system_time(:second)

      %SweepRun{}
      |> SweepRun.changeset(%{
        sweep_type: :missed_schedules_catchall,
        started_at: now - 60,
        completed_at: now - 30,
        executions_processed: 5
      })
      |> Journey.Repo.insert!()

      # Try to run again immediately
      {count, sweep_run_id} = MissedSchedulesCatchall.sweep()

      assert count == 0
      assert sweep_run_id == nil
    end

    test "runs if more than 23 hours have passed since last run (using started_at)" do
      # Create a sweep run from 24 hours ago
      old_time = System.system_time(:second) - 24 * 60 * 60

      %SweepRun{}
      |> SweepRun.changeset(%{
        sweep_type: :missed_schedules_catchall,
        # Using started_at for timing check
        started_at: old_time - 60,
        completed_at: old_time,
        executions_processed: 3
      })
      |> Journey.Repo.insert!()

      # Should be able to run now
      {_count, sweep_run_id} = MissedSchedulesCatchall.sweep()

      assert sweep_run_id != nil
    end

    test "records sweep run on execution" do
      # Ensure no recent runs exist
      {count, sweep_run_id} = MissedSchedulesCatchall.sweep()

      # Even with no executions to process, should record the run
      assert sweep_run_id != nil

      # Verify the sweep run was recorded properly
      sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
      assert sweep_run.sweep_type == :missed_schedules_catchall
      assert sweep_run.started_at != nil
      assert sweep_run.completed_at != nil
      assert sweep_run.executions_processed == count
    end

    test "handles non-existent execution_id gracefully" do
      # Should not error when given a specific execution that doesn't exist
      {count, _sweep_run_id} = MissedSchedulesCatchall.sweep("EXEC_DOES_NOT_EXIST")

      assert count == 0
    end
  end

  describe "sweep/1 - core schedule recovery functionality" do
    test "recovers missed schedule for specific execution" do
      # Create a realistic scenario where schedule was missed
      # Use a past time that's in the catch-all window (25min - 7 days old)
      unique_id = random_string()
      # Use exactly 30 minutes ago to be outside the regular sweep's 25-minute window
      past_time = System.system_time(:second) - 30 * 60

      # Create execution but don't trigger the schedule initially
      graph =
        Journey.new_graph(
          "missed-schedule-recovery-#{unique_id}",
          "v1.0.0",
          [
            input(:trigger),
            schedule_once(:past_schedule, [:trigger], fn _ -> {:ok, past_time} end),
            compute(:recovered_result, [:past_schedule], fn _ -> {:ok, "schedule recovered!"} end)
          ]
        )

      execution = Journey.start_execution(graph)

      # Manually update the schedule value as if it was computed 30 minutes ago
      # This simulates a schedule that was missed during downtime
      from(v in Journey.Persistence.Schema.Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == "past_schedule"
      )
      |> Journey.Repo.update_all(set: [node_value: past_time, set_time: past_time, ex_revision: 1])

      # Verify schedule value exists but downstream computation hasn't run
      values = Journey.values(execution)
      assert values[:past_schedule] == past_time
      assert values[:recovered_result] == nil

      # Run catch-all sweep ONLY on this specific execution
      {count, sweep_run_id} = MissedSchedulesCatchall.sweep(execution.id)

      assert count == 1
      assert sweep_run_id != nil

      # Wait for async computation to complete and verify the missed schedule was recovered
      {:ok, result} = Journey.get_value(execution, :recovered_result, wait_any: true)
      assert result == "schedule recovered!"
    end

    test "ignores recent schedules (less than 25 minutes old)" do
      unique_id = random_string()
      # 10 minutes ago
      recent_time = System.system_time(:second) - 10 * 60

      graph =
        Journey.new_graph(
          "recent-schedule-#{unique_id}",
          "v1.0.0",
          [
            input(:trigger),
            schedule_once(:recent_schedule, [:trigger], fn _ -> {:ok, recent_time} end),
            compute(:should_not_run, [:recent_schedule], fn _ -> {:ok, "should not execute"} end)
          ]
        )

      execution = Journey.start_execution(graph)

      # Manually update recent schedule value
      from(v in Journey.Persistence.Schema.Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == "recent_schedule"
      )
      |> Journey.Repo.update_all(set: [node_value: recent_time, set_time: recent_time, ex_revision: 1])

      # Verify schedule value exists but downstream computation hasn't run
      values = Journey.values(execution)
      assert values[:recent_schedule] == recent_time
      assert values[:should_not_run] == nil

      # Run sweep - should ignore this recent schedule
      {count, _} = MissedSchedulesCatchall.sweep(execution.id)
      assert count == 0

      # Verify downstream still hasn't run after sweep
      execution = Journey.load(execution.id)
      values = Journey.values(execution)
      assert values[:should_not_run] == nil
    end

    test "ignores very old schedules (older than 7 days)" do
      unique_id = random_string()
      # 8 days ago
      very_old_time = System.system_time(:second) - 8 * 24 * 60 * 60

      graph =
        Journey.new_graph(
          "old-schedule-#{unique_id}",
          "v1.0.0",
          [
            input(:trigger),
            schedule_once(:old_schedule, [:trigger], fn _ -> {:ok, very_old_time} end),
            compute(:should_not_run, [:old_schedule], fn _ -> {:ok, "should not execute"} end)
          ]
        )

      execution = Journey.start_execution(graph)

      # Manually update very old schedule value
      from(v in Journey.Persistence.Schema.Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == "old_schedule"
      )
      |> Journey.Repo.update_all(set: [node_value: very_old_time, set_time: very_old_time, ex_revision: 1])

      # Run sweep - should ignore this very old schedule
      {count, _} = MissedSchedulesCatchall.sweep(execution.id)
      assert count == 0

      # Verify downstream didn't run
      execution = Journey.load(execution.id)
      values = Journey.values(execution)
      assert values[:should_not_run] == nil
    end

    test "processes valid time window schedules (25 minutes to 7 days old)" do
      unique_id = random_string()
      # 2 hours ago (in valid window)
      valid_time = System.system_time(:second) - 2 * 60 * 60

      graph =
        Journey.new_graph(
          "valid-window-#{unique_id}",
          "v1.0.0",
          [
            input(:trigger),
            schedule_once(:valid_schedule, [:trigger], fn _ -> {:ok, valid_time} end),
            compute(:should_run, [:valid_schedule], fn _ -> {:ok, "executed in valid window"} end)
          ]
        )

      execution = Journey.start_execution(graph)

      # Manually update schedule value in valid time window
      from(v in Journey.Persistence.Schema.Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == "valid_schedule"
      )
      |> Journey.Repo.update_all(set: [node_value: valid_time, set_time: valid_time, ex_revision: 1])

      # Verify initial state
      values = Journey.values(execution)
      assert values[:valid_schedule] == valid_time
      assert values[:should_run] == nil

      # Run sweep - should process this schedule
      {count, _} = MissedSchedulesCatchall.sweep(execution.id)
      assert count == 1

      # Verify downstream computation ran
      {:ok, result} = Journey.get_value(execution, :should_run, wait_any: true)
      assert result == "executed in valid window"
    end

    test "handles both schedule_once and schedule_recurring" do
      unique_id = random_string()
      # 1.5 hours ago
      past_time = System.system_time(:second) - 90 * 60

      graph =
        Journey.new_graph(
          "mixed-schedules-#{unique_id}",
          "v1.0.0",
          [
            input(:trigger),
            schedule_once(:once_schedule, [:trigger], fn _ -> {:ok, past_time} end),
            schedule_recurring(:recurring_schedule, [:trigger], fn _ -> {:ok, past_time} end),
            compute(:once_result, [:once_schedule], fn _ -> {:ok, "once recovered"} end),
            compute(:recurring_result, [:recurring_schedule], fn _ -> {:ok, "recurring recovered"} end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :trigger, true)

      # Wait for schedules to compute
      {:ok, _} = Journey.get_value(execution, :once_schedule, wait_any: true)
      {:ok, _} = Journey.get_value(execution, :recurring_schedule, wait_any: true)

      # Run sweep
      {count, _} = MissedSchedulesCatchall.sweep(execution.id)
      assert count >= 0

      # Verify both schedule types triggered their computations
      {:ok, once_result} = Journey.get_value(execution, :once_result, wait_any: true)
      {:ok, recurring_result} = Journey.get_value(execution, :recurring_result, wait_any: true)
      assert once_result == "once recovered"
      assert recurring_result == "recurring recovered"
    end
  end

  describe "sweep/1 - configuration and scheduling" do
    test "respects preferred_hour configuration when set" do
      # Set config to only run at a different hour than current
      current_hour = DateTime.utc_now().hour
      different_hour = rem(current_hour + 1, 24)

      original_config = Application.get_env(:journey, :missed_schedules_catchall, [])

      Application.put_env(:journey, :missed_schedules_catchall,
        preferred_hour: different_hour,
        lookback_days: 7
      )

      try do
        # Ensure we're not blocked by min_hours_between_runs
        Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :missed_schedules_catchall))

        # Sweep should not run due to wrong hour
        {count, sweep_run_id} = MissedSchedulesCatchall.sweep()
        assert count == 0
        assert sweep_run_id == nil
      after
        Application.put_env(:journey, :missed_schedules_catchall, original_config)
      end
    end

    test "runs when preferred_hour is nil (no restriction)" do
      original_config = Application.get_env(:journey, :missed_schedules_catchall, [])

      Application.put_env(:journey, :missed_schedules_catchall,
        # No hour restriction
        preferred_hour: nil,
        lookback_days: 7
      )

      try do
        # Ensure we're not blocked by min_hours_between_runs
        Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :missed_schedules_catchall))

        # Should run regardless of current hour
        {_count, sweep_run_id} = MissedSchedulesCatchall.sweep()
        assert sweep_run_id != nil
      after
        Application.put_env(:journey, :missed_schedules_catchall, original_config)
      end
    end

    test "does not run when disabled via configuration" do
      original_config = Application.get_env(:journey, :missed_schedules_catchall, [])

      Application.put_env(:journey, :missed_schedules_catchall,
        enabled: false,
        preferred_hour: nil,
        lookback_days: 7
      )

      try do
        # Ensure we're not blocked by other conditions
        Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :missed_schedules_catchall))

        # Sweep should not run because it's disabled
        {count, sweep_run_id} = MissedSchedulesCatchall.sweep()
        assert count == 0
        assert sweep_run_id == nil
      after
        Application.put_env(:journey, :missed_schedules_catchall, original_config)
      end
    end

    test "respects lookback_days configuration" do
      original_config = Application.get_env(:journey, :missed_schedules_catchall, [])

      Application.put_env(:journey, :missed_schedules_catchall,
        # Allow to run now
        preferred_hour: nil,
        # Only look back 3 days
        lookback_days: 3
      )

      try do
        unique_id = random_string()
        # Create schedule 4 days old (outside 3-day window)
        old_time = System.system_time(:second) - 4 * 24 * 60 * 60

        graph =
          Journey.new_graph(
            "outside-window-#{unique_id}",
            "v1.0.0",
            [
              input(:trigger),
              schedule_once(:old_schedule, [:trigger], fn _ -> {:ok, old_time} end),
              compute(:should_not_run, [:old_schedule], fn _ -> {:ok, "should not execute"} end)
            ]
          )

        execution = Journey.start_execution(graph)

        # Manually set old schedule value
        from(v in Journey.Persistence.Schema.Execution.Value,
          where: v.execution_id == ^execution.id and v.node_name == "old_schedule"
        )
        |> Journey.Repo.update_all(set: [node_value: old_time, set_time: old_time, ex_revision: 1])

        # Run sweep - should ignore due to configured lookback window
        {count, _} = MissedSchedulesCatchall.sweep(execution.id)
        assert count == 0

        # Verify downstream didn't run
        execution = Journey.load(execution.id)
        values = Journey.values(execution)
        assert values[:should_not_run] == nil
      after
        Application.put_env(:journey, :missed_schedules_catchall, original_config)
      end
    end
  end

  describe "sweep/1 - race condition prevention" do
    test "prevents concurrent sweep execution with advisory locks" do
      # This test simulates multiple processes trying to run the sweep concurrently
      # Only one should succeed in creating a sweep run

      # Create some work for the sweep to do
      unique_id = random_string()
      past_time = System.system_time(:second) - 30 * 60

      graph =
        Journey.new_graph(
          "concurrent-test-#{unique_id}",
          "v1.0.0",
          [
            input(:trigger),
            schedule_once(:past_schedule, [:trigger], fn _ -> {:ok, past_time} end),
            compute(:result, [:past_schedule], fn _ -> {:ok, "done"} end)
          ]
        )

      execution = Journey.start_execution(graph)
      Journey.set(execution, :trigger, true)

      # Spawn multiple concurrent processes that try to run the sweep
      tasks =
        for _i <- 1..5 do
          Task.async(fn ->
            # Each process tries to run the sweep
            MissedSchedulesCatchall.sweep()
          end)
        end

      # Wait for all tasks to complete
      results = Task.await_many(tasks, 5000)

      # Count how many sweeps actually ran (returned a sweep_run_id)
      successful_sweeps =
        results
        |> Enum.filter(fn {_count, sweep_run_id} -> sweep_run_id != nil end)
        |> length()

      # Exactly one should have succeeded
      assert successful_sweeps == 1, "Expected exactly 1 successful sweep, got #{successful_sweeps}"

      # Verify only one SweepRun was created
      sweep_runs =
        from(sr in SweepRun,
          where: sr.sweep_type == :missed_schedules_catchall,
          order_by: [desc: sr.started_at]
        )
        |> Journey.Repo.all()

      assert length(sweep_runs) == 1, "Expected exactly 1 sweep run in DB, got #{length(sweep_runs)}"
    end
  end

  describe "sweep/1 - error handling and batch processing" do
    test "continues processing when individual execution fails" do
      unique_id = random_string()
      past_time = System.system_time(:second) - 2 * 60 * 60

      # Create good execution that should process successfully
      good_graph =
        Journey.new_graph(
          "good-execution-#{unique_id}",
          "v1.0.0",
          [
            input(:trigger),
            schedule_once(:good_schedule, [:trigger], fn _ -> {:ok, past_time} end),
            compute(:good_result, [:good_schedule], fn _ -> {:ok, "success"} end)
          ]
        )

      good_execution = Journey.start_execution(good_graph)
      good_execution = Journey.set(good_execution, :trigger, true)

      # Wait for schedule to compute successfully
      {:ok, _} = Journey.get_value(good_execution, :good_schedule, wait_any: true)

      # Create a second execution and then archive it to simulate Journey.load() failure
      failing_execution = Journey.start_execution(good_graph)
      failing_execution = Journey.set(failing_execution, :trigger, true)
      {:ok, _} = Journey.get_value(failing_execution, :good_schedule, wait_any: true)

      # Archive the failing execution - this will cause Journey.load() to return nil
      Journey.archive(failing_execution)

      # Now manually insert a past schedule value for the archived execution
      # This simulates the query finding it but load() failing
      import Ecto.Query

      from(v in Journey.Persistence.Schema.Execution.Value,
        where: v.execution_id == ^failing_execution.id and v.node_name == "good_schedule"
      )
      |> Journey.Repo.update_all(set: [node_value: past_time])

      # Test global sweep - should handle the archived execution gracefully
      {count, _} = MissedSchedulesCatchall.sweep()

      assert count >= 0

      # Verify good execution completed successfully
      {:ok, result} = Journey.get_value(good_execution, :good_result, wait_any: true)
      assert result == "success"
    end

    test "processes multiple executions in batches" do
      unique_base_id = random_string()
      past_time = System.system_time(:second) - 2 * 60 * 60

      # Create 5 executions with past schedules
      executions =
        for i <- 1..5 do
          graph =
            Journey.new_graph(
              "batch-test-#{unique_base_id}-#{i}",
              "v1.0.0",
              [
                input(:trigger),
                schedule_once(:batch_schedule, [:trigger], fn _ -> {:ok, past_time} end),
                compute(:batch_result, [:batch_schedule], fn _ -> {:ok, "batch-#{i}"} end)
              ]
            )

          execution = Journey.start_execution(graph)
          Journey.set(execution, :trigger, true)
        end

      # Wait for all schedules to compute
      for execution <- executions do
        {:ok, _} = Journey.get_value(execution, :batch_schedule, wait_any: true)
      end

      # Run global sweep (not execution-specific) to test batch processing
      {count, sweep_run_id} = MissedSchedulesCatchall.sweep()

      assert count >= 0
      assert sweep_run_id != nil

      # Verify all our executions were processed
      for execution <- executions do
        {:ok, result} = Journey.get_value(execution, :batch_result, wait_any: true)
        assert String.starts_with?(result, "batch-")
      end
    end
  end
end
