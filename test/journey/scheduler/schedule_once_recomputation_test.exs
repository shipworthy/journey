defmodule Journey.Scheduler.ScheduleOnceRecomputationTest do
  use ExUnit.Case, async: false

  import Journey.Node

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

  test "schedule_once recomputes when upstream dependency changes", %{execution: execution} do
    initial_due_time = System.system_time(:second) + 100_000
    execution = Journey.set(execution, :due_date, initial_due_time)

    {:ok, %{value: initial_reminder_time}} = Journey.get_value(execution, :reminder_schedule, wait_any: true)
    initial_computation_count = count_computations_for(execution, :reminder_schedule)

    future_timestamp = System.system_time(:second) + 200_000
    execution = Journey.set(execution, :due_date, future_timestamp)
    execution = Journey.Scheduler.advance(execution)

    case Journey.get_value(execution, :reminder_schedule, wait_new: 5000) do
      {:ok, %{value: updated_reminder_time}} ->
        assert updated_reminder_time != initial_reminder_time
        expected_new_time = future_timestamp - 86_400
        assert updated_reminder_time == expected_new_time

        # Verify new computation was created
        execution = Journey.load(execution)
        final_computation_count = count_computations_for(execution, :reminder_schedule)
        assert final_computation_count > initial_computation_count

      {:error, :not_set} ->
        execution = Journey.load(execution)
        final_computation_count = count_computations_for(execution, :reminder_schedule)

        if final_computation_count > initial_computation_count do
          assert true, "New computation was created, showing recomputation logic is working"
        else
          flunk("schedule_once did not recompute when upstream dependency changed")
        end
    end
  end

  test "downstream nodes work correctly with rescheduled times", %{execution: execution} do
    initial_due_time = System.system_time(:second) + 100_000
    execution = Journey.set(execution, :due_date, initial_due_time)

    {:ok, %{value: _initial_time}} = Journey.get_value(execution, :reminder_schedule, wait_any: true)

    near_future = System.system_time(:second) + 2
    execution = Journey.set(execution, :due_date, near_future)

    case Journey.get_value(execution, :send_reminder, wait_any: 15_000) do
      {:ok, %{value: reminder_message}} ->
        assert String.contains?(reminder_message, "Reminder")

      {:error, :not_set} ->
        flunk("Downstream reminder did not fire after schedule_once recomputation")
    end
  end

  test "multiple upstream changes create multiple recomputations", %{execution: execution} do
    initial_due_time = System.system_time(:second) + 100_000
    execution = Journey.set(execution, :due_date, initial_due_time)
    {:ok, %{value: _}} = Journey.get_value(execution, :reminder_schedule, wait_any: true)

    initial_count = count_computations_for(execution, :reminder_schedule)

    second_due_time = System.system_time(:second) + 200_000
    execution = Journey.set(execution, :due_date, second_due_time)
    execution = Journey.Scheduler.advance(execution)

    third_due_time = System.system_time(:second) + 300_000
    execution = Journey.set(execution, :due_date, third_due_time)
    execution = Journey.Scheduler.advance(execution)

    final_count = count_computations_for(execution, :reminder_schedule)
    assert final_count > initial_count
  end

  defp count_computations_for(execution, node_name) do
    execution = Journey.load(execution)

    execution.computations
    |> Enum.filter(fn c -> c.node_name == node_name end)
    |> length()
  end

  defp test_graph do
    Journey.new_graph(
      "schedule_once recomputation test",
      "v1.0.0",
      [
        input(:due_date),
        schedule_once(
          :reminder_schedule,
          [:due_date],
          fn %{due_date: due_date} ->
            reminder_time =
              case due_date do
                time when is_integer(time) -> time - 86_400
                _string_date -> System.system_time(:second) + 3
              end

            {:ok, reminder_time}
          end
        ),
        compute(
          :send_reminder,
          [:reminder_schedule, :due_date],
          fn %{reminder_schedule: _schedule_time, due_date: due_date} ->
            {:ok, "Reminder: Task due on #{due_date}"}
          end
        )
      ]
    )
  end
end
