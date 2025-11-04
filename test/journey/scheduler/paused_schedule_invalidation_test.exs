defmodule Journey.Scheduler.PausedScheduleInvalidationTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Test.Support.Helpers

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  @moduletag timeout: 60_000

  describe "invalidation with paused schedule_recurring" do
    test "paused schedule + valid other dependency - should NOT clear downstream" do
      graph =
        Journey.new_graph(
          "paused schedule with valid dep test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_config),
            input(:enable_schedule),
            tick_recurring(
              :schedule_pulse,
              [:enable_schedule],
              fn %{enable_schedule: enabled} ->
                if enabled do
                  {:ok, System.system_time(:second) + 2}
                else
                  {:ok, 0}
                end
              end
            ),
            compute(
              :process_data,
              [:user_config, :schedule_pulse],
              fn %{user_config: config} = v ->
                count = Map.get(v, :process_data, 0) + 1
                {:ok, "#{config}-#{count}"}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Start with schedule enabled and config set
      execution = Journey.set(execution, :user_config, "v1")
      execution = Journey.set(execution, :enable_schedule, true)

      # Wait for first execution
      assert wait_for_value(execution, :process_data, "v1-1", timeout: 10_000)

      # Pause the schedule
      execution = Journey.set(execution, :enable_schedule, false)
      assert {:ok, 0, _} = Journey.get(execution, :schedule_pulse, wait: :newer)

      # Reload to get latest state
      execution = Journey.load(execution)

      # CRITICAL: process_data should still have its value (user_config is still valid)
      assert {:ok, "v1-1", _} = Journey.get(execution, :process_data),
             "Downstream should keep value when schedule pauses but other deps remain valid"

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "paused schedule + invalid other dependency - SHOULD clear downstream" do
      graph =
        Journey.new_graph(
          "paused schedule with invalid dep test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_config),
            input(:enable_schedule),
            tick_recurring(
              :schedule_pulse,
              [:enable_schedule],
              fn %{enable_schedule: enabled} ->
                if enabled do
                  {:ok, System.system_time(:second) + 2}
                else
                  {:ok, 0}
                end
              end
            ),
            compute(
              :process_data,
              [:user_config, :schedule_pulse],
              fn %{user_config: config} = v ->
                count = Map.get(v, :process_data, 0) + 1
                {:ok, "#{config}-#{count}"}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Start with schedule enabled and config set
      execution = Journey.set(execution, :user_config, "v1")
      execution = Journey.set(execution, :enable_schedule, true)

      # Wait for first execution
      assert wait_for_value(execution, :process_data, "v1-1", timeout: 10_000)

      # Pause schedule AND unset config
      execution = Journey.set(execution, :enable_schedule, false)
      execution = Journey.unset(execution, :user_config)

      # Reload to get latest state
      execution = Journey.load(execution)

      # CRITICAL: process_data should be cleared (user_config is invalid)
      assert {:error, :not_set} = Journey.get(execution, :process_data),
             "Downstream should be cleared when other dependency becomes invalid, even if schedule is paused"

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "paused schedule as only dependency - should NOT clear downstream" do
      # This is the main scenario from the failing test
      graph =
        Journey.new_graph(
          "paused schedule only dep test #{__MODULE__}",
          "1.0.0",
          [
            input(:enable_schedule),
            tick_recurring(
              :schedule_pulse,
              [:enable_schedule],
              fn %{enable_schedule: enabled} ->
                if enabled do
                  {:ok, System.system_time(:second) + 2}
                else
                  {:ok, 0}
                end
              end
            ),
            compute(
              :counter,
              [:schedule_pulse],
              fn v ->
                count = Map.get(v, :counter, 0) + 1
                {:ok, count}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Start with schedule enabled
      execution = Journey.set(execution, :enable_schedule, true)

      # Wait for first execution
      assert wait_for_value(execution, :counter, 1, timeout: 10_000)

      # Pause the schedule
      execution = Journey.set(execution, :enable_schedule, false)
      assert {:ok, 0, _} = Journey.get(execution, :schedule_pulse, wait: :newer)

      # Reload to get latest state
      execution = Journey.load(execution)

      # CRITICAL: counter should still have its value
      assert {:ok, 1, _} = Journey.get(execution, :counter),
             "Downstream should keep accumulated value when schedule pauses"

      # Wait to ensure no new executions
      :timer.sleep(3_000)
      execution = Journey.load(execution)

      assert {:ok, 1, _} = Journey.get(execution, :counter),
             "Counter should not increment while paused"

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "resuming paused schedule allows execution to continue" do
      graph =
        Journey.new_graph(
          "resume paused schedule test #{__MODULE__}",
          "1.0.0",
          [
            input(:enable_schedule),
            tick_recurring(
              :schedule_pulse,
              [:enable_schedule],
              fn %{enable_schedule: enabled} ->
                if enabled do
                  {:ok, System.system_time(:second) + 2}
                else
                  {:ok, 0}
                end
              end
            ),
            compute(
              :counter,
              [:schedule_pulse],
              fn v ->
                count = Map.get(v, :counter, 0) + 1
                {:ok, count}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Start with schedule enabled
      execution = Journey.set(execution, :enable_schedule, true)
      assert wait_for_value(execution, :counter, 1, timeout: 10_000)

      # Pause
      execution = Journey.set(execution, :enable_schedule, false)
      assert {:ok, 0, _} = Journey.get(execution, :schedule_pulse, wait: :newer)

      # Verify counter is preserved
      execution = Journey.load(execution)
      {:ok, paused_count, _} = Journey.get(execution, :counter)
      assert paused_count >= 1

      # Resume
      execution = Journey.set(execution, :enable_schedule, true)

      # Verify execution continues from where it left off
      assert wait_for_value(execution, :counter, paused_count + 1, timeout: 10_000),
             "Counter should continue incrementing from paused value after resume"

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end
end
