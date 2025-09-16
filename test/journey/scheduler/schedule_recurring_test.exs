defmodule Journey.Scheduler.Scheduler.ScheduleRecurringTest do
  use ExUnit.Case, async: true

  require Logger

  import Journey.Helpers.Log
  import Journey.Test.Support.Helpers

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  @tag timeout: 60_000
  test "basic schedule_recurring flow" do
    graph = graph()
    execution = graph |> Journey.start_execution()
    background_sweeps_task = start_background_sweeps_in_test(execution.id)

    execution = execution |> Journey.set(:user_name, "Mario")

    assert Journey.values_all(execution) |> redact([:last_updated_at]) == %{
             execution_id: {:set, execution.id},
             keep_sending_reminders: :not_set,
             last_updated_at: {:set, 1_234_567_890},
             send_a_reminder: :not_set,
             schedule_a_reminder: :not_set,
             user_name: {:set, "Mario"}
           }

    expected_scheduled_time = System.system_time(:second) + 10
    execution = execution |> Journey.set(:keep_sending_reminders, true)

    {:ok, original_scheduled_time} = Journey.get_value(execution, :schedule_a_reminder, wait_any: true)

    assert original_scheduled_time in (expected_scheduled_time - 1)..(expected_scheduled_time + 5)

    assert Journey.values_all(execution) |> redact([:last_updated_at, :schedule_a_reminder]) == %{
             execution_id: {:set, execution.id},
             keep_sending_reminders: {:set, true},
             last_updated_at: {:set, 1_234_567_890},
             send_a_reminder: :not_set,
             schedule_a_reminder: {:set, 1_234_567_890},
             user_name: {:set, "Mario"}
           }

    assert Journey.load(execution).computations |> Enum.count() == 2

    assert wait_for_value(execution, :send_a_reminder, 1, frequency: 1_000)
    assert System.system_time(:second) >= original_scheduled_time

    assert wait_for_value(execution, :send_a_reminder, 2, frequency: 1_000)
    assert System.system_time(:second) >= original_scheduled_time + 10

    stop_background_sweeps_in_test(background_sweeps_task)
  end

  defp graph() do
    Journey.new_graph(
      "test graph #{__MODULE__}",
      "1.0.0",
      [
        input(:user_name),
        input(:keep_sending_reminders),
        schedule_recurring(
          :schedule_a_reminder,
          unblocked_when({
            :and,
            [{:user_name, &provided?/1}, {:keep_sending_reminders, &true?/1}]
          }),
          &schedule_next_reminder/1
        ),
        compute(
          :send_a_reminder,
          unblocked_when({
            :and,
            [{:keep_sending_reminders, &true?/1}, {:schedule_a_reminder, &provided?/1}]
          }),
          &send_reminder/1
        )
      ]
    )
  end

  defp schedule_next_reminder(%{execution_id: execution_id}) do
    next_reminder_time = System.system_time(:second) + 10
    Logger.debug("[#{execution_id}] [#{mf()}]: scheduling for '#{DateTime.from_unix!(next_reminder_time)}'")
    {:ok, next_reminder_time}
  end

  defp send_reminder(%{user_name: user_name, execution_id: execution_id} = v) do
    reminder_count = Map.get(v, :send_a_reminder, 0) + 1
    reminder = "Here is your reminder #{reminder_count}, #{user_name}"
    Logger.debug("[#{execution_id}] [#{mf()}]: '#{reminder}'")
    {:ok, reminder_count}
  end
end
