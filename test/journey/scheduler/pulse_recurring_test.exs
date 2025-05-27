defmodule Journey.Scheduler.Scheduler.PulseRecurringTest do
  use ExUnit.Case, async: true

  require Logger

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  alias Journey.Scheduler.BackgroundSweeps

  test "basic schedule_recurring flow" do
    graph = graph()
    execution = graph |> Journey.start_execution()
    background_sweeps_task = BackgroundSweeps.start_background_sweeps_in_test(execution.id)

    execution = execution |> Journey.set_value(:user_name, "Mario")
    Process.sleep(5_000)

    assert Journey.values_all(execution) |> redact([:last_updated_at]) == %{
             execution_id: {:set, execution.id},
             keep_sending_reminders: :not_set,
             last_updated_at: {:set, 1_234_567_890},
             send_a_reminder: :not_set,
             schedule_a_reminder: :not_set,
             user_name: {:set, "Mario"}
           }

    execution = execution |> Journey.set_value(:keep_sending_reminders, true)

    # A reminder should get scheduled into the future, but not yet executed.
    {:ok, original_scheduled_time} = Journey.get_value(execution, :schedule_a_reminder, wait: true)
    assert original_scheduled_time > System.system_time(:second)

    assert Journey.values_all(execution) |> redact([:last_updated_at, :schedule_a_reminder]) == %{
             execution_id: {:set, execution.id},
             keep_sending_reminders: {:set, true},
             last_updated_at: {:set, 1_234_567_890},
             send_a_reminder: :not_set,
             schedule_a_reminder: {:set, 1_234_567_890},
             user_name: {:set, "Mario"}
           }

    assert Journey.load(execution).computations |> Enum.count() == 2

    # Wait for that future to arrive and watch the first reminder get generated.
    Process.sleep(original_scheduled_time - System.system_time(:second) + 1)
    assert {:ok, 1} = Journey.get_value(execution, :send_a_reminder, wait: 50_000)

    new_state = Journey.values(execution)

    assert new_state |> redact([:last_updated_at, :schedule_a_reminder]) == %{
             execution_id: execution.id,
             keep_sending_reminders: true,
             last_updated_at: 1_234_567_890,
             schedule_a_reminder: 1_234_567_890,
             send_a_reminder: 1,
             user_name: "Mario"
           }

    assert new_state.schedule_a_reminder >= original_scheduled_time + 10
    Process.sleep(1000)

    # Another scheduled time got picked.
    {:ok, scheduled_time2} = Journey.get_value(execution, :schedule_a_reminder, wait: true)
    assert scheduled_time2 == new_state.schedule_a_reminder
    assert scheduled_time2 > System.system_time(:second)
    Process.sleep(scheduled_time2 - System.system_time(:second) + 5)
    Journey.Tools.summarize(execution.id) |> IO.puts()
    Process.sleep(10_000)
    assert {:ok, 2} = Journey.get_value(execution, :send_a_reminder, wait: 50_000)

    BackgroundSweeps.stop_background_sweeps_in_test(background_sweeps_task)
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
    Logger.error("schedule_next_reminder[#{execution_id}]: scheduling for '#{DateTime.from_unix!(next_reminder_time)}'")
    {:ok, next_reminder_time}
  end

  defp send_reminder(%{user_name: user_name, execution_id: execution_id} = v) do
    reminder_count = Map.get(v, :send_a_reminder, 0) + 1
    reminder = "Here is your reminder #{reminder_count}, #{user_name}"
    Logger.error("send_reminder[#{execution_id}][#{reminder_count}]: '#{reminder}'")
    {:ok, reminder_count}
  end
end
