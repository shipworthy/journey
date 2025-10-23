defmodule Journey.Scheduler.ScheduleRecurringRecomputationTest do
  use ExUnit.Case, async: false

  import Journey.Node
  import Journey.Test.Support.Helpers

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  setup do
    graph = test_graph()
    execution = Journey.start_execution(graph)
    background_sweeps_task = start_background_sweeps_in_test(execution.id)

    on_exit(fn ->
      stop_background_sweeps_in_test(background_sweeps_task)
    end)

    {:ok, execution: execution, graph: graph}
  end

  @tag timeout: 60_000
  test "schedule_recurring recomputes when upstream dependency changes", %{execution: execution} do
    # Set initial interval to 100 seconds (far in the future)
    execution = Journey.set(execution, :interval_seconds, 100)

    {:ok, initial_scheduled_time, _} =
      Journey.get(execution, :recurring_schedule, wait: :any)

    initial_computation_count = count_computations_for(execution, :recurring_schedule)

    expected_initial_time = System.system_time(:second) + 100
    assert initial_scheduled_time in (expected_initial_time - 2)..(expected_initial_time + 5)

    # Change interval to 200 seconds
    execution = Journey.set(execution, :interval_seconds, 200)
    execution = Journey.Scheduler.advance(execution)

    case Journey.get(execution, :recurring_schedule, wait: :newer, timeout: 5000) do
      {:ok, updated_scheduled_time, _} ->
        assert updated_scheduled_time != initial_scheduled_time
        expected_new_time = System.system_time(:second) + 200
        assert updated_scheduled_time in (expected_new_time - 2)..(expected_new_time + 10)

        # Verify new computation was created
        execution = Journey.load(execution)
        final_computation_count = count_computations_for(execution, :recurring_schedule)
        assert final_computation_count > initial_computation_count

      {:error, :not_set} ->
        execution = Journey.load(execution)
        final_computation_count = count_computations_for(execution, :recurring_schedule)

        if final_computation_count > initial_computation_count do
          assert true, "New computation was created, showing recomputation logic is working"
        else
          flunk("schedule_recurring did not recompute when upstream dependency changed")
        end
    end
  end

  @tag timeout: 60_000
  test "recurring behavior continues after recomputation", %{execution: execution} do
    # Start with short interval for testing
    execution = Journey.set(execution, :interval_seconds, 3)

    {:ok, _first_time, _} = Journey.get(execution, :recurring_schedule, wait: :any)

    # Wait for first downstream execution
    assert wait_for_value(execution, :downstream_result, 1, frequency: 500, max_wait: 15_000)

    # Change the interval - this should trigger recomputation
    execution = Journey.set(execution, :interval_seconds, 4)
    execution = Journey.Scheduler.advance(execution)

    # Wait for new scheduled time to be computed with updated interval
    :timer.sleep(1_000)

    # Verify recurring behavior continues - should fire at least one more time
    assert wait_for_value(execution, :downstream_result, 2, frequency: 500, max_wait: 15_000)

    # And another time to confirm it keeps recurring
    assert wait_for_value(execution, :downstream_result, 3, frequency: 500, max_wait: 15_000)
  end

  @tag timeout: 60_000
  test "configuration-driven recurring schedule responds to changes", %{execution: execution} do
    # Test a realistic scenario: notification frequency changes

    # Start with daily notifications (86400 seconds)
    daily_interval = 86_400
    execution = Journey.set(execution, :interval_seconds, daily_interval)

    {:ok, initial_time, _} = Journey.get(execution, :recurring_schedule, wait: :any)
    expected_daily = System.system_time(:second) + daily_interval
    assert initial_time in (expected_daily - 2)..(expected_daily + 5)

    initial_computation_count = count_computations_for(execution, :recurring_schedule)

    # User changes preference to hourly notifications (3600 seconds)
    hourly_interval = 3_600
    execution = Journey.set(execution, :interval_seconds, hourly_interval)
    execution = Journey.Scheduler.advance(execution)

    case Journey.get(execution, :recurring_schedule, wait: :newer, timeout: 5000) do
      {:ok, updated_time, _} ->
        # Should now be scheduled hourly, not daily
        expected_hourly = System.system_time(:second) + hourly_interval
        assert updated_time in (expected_hourly - 2)..(expected_hourly + 10)

        # Verify the change was immediate (new computation created)
        execution = Journey.load(execution)
        final_computation_count = count_computations_for(execution, :recurring_schedule)
        assert final_computation_count > initial_computation_count

      {:error, :not_set} ->
        execution = Journey.load(execution)
        final_computation_count = count_computations_for(execution, :recurring_schedule)

        if final_computation_count > initial_computation_count do
          assert true, "Configuration change triggered recomputation"
        else
          flunk("Configuration change did not trigger recomputation")
        end
    end
  end

  defp count_computations_for(execution, node_name) do
    execution = Journey.load(execution)

    execution.computations
    |> Enum.filter(fn c -> c.node_name == node_name end)
    |> length()
  end

  defp test_graph do
    Journey.new_graph(
      "schedule_recurring recomputation test",
      "v1.0.0",
      [
        input(:interval_seconds),
        schedule_recurring(
          :recurring_schedule,
          [:interval_seconds],
          fn %{interval_seconds: interval} ->
            next_time = System.system_time(:second) + interval
            {:ok, next_time}
          end
        ),
        compute(
          :downstream_result,
          [:recurring_schedule, :interval_seconds],
          fn %{interval_seconds: _interval} = v ->
            count = Map.get(v, :downstream_result, 0) + 1
            {:ok, count}
          end
        )
      ]
    )
  end
end
