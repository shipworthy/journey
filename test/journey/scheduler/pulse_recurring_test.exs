defmodule Journey.Scheduler.Scheduler.PulseRecurringTest do
  use ExUnit.Case, async: true

  import Journey.Node
  alias Journey.Scheduler.BackgroundSweep

  test "basic schedule_recurring flow" do
    # TODO: implement
    graph = simple_graph()
    execution = graph |> Journey.start_execution()

    execution = execution |> Journey.set_value(:user_name, "Mario")
    BackgroundSweep.find_and_kick_recently_due_schedule_values(execution.id)

    assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Mario"}

    Process.sleep(1000)
    BackgroundSweep.find_and_kick_recently_due_schedule_values(execution.id)
    {:ok, _time} = Journey.get_value(execution, :time_to_issue_reminder_schedule_recurring, wait: true)

    BackgroundSweep.find_and_kick_recently_due_schedule_values(execution.id)
    Process.sleep(1000)
    BackgroundSweep.find_and_kick_recently_due_schedule_values(execution.id)
    Process.sleep(1000)
    BackgroundSweep.find_and_kick_recently_due_schedule_values(execution.id)
    Process.sleep(1000)
    BackgroundSweep.find_and_kick_recently_due_schedule_values(execution.id)
    assert {:ok, "Reminder: Hello, Mario"} = Journey.get_value(execution, :reminder, wait: true)

    assert Journey.values(execution) |> redact(:time_to_issue_reminder_schedule_recurring) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             reminder: "Reminder: Hello, Mario",
             time_to_issue_reminder_schedule_recurring: :redacted
           }
  end

  defp redact(map, key) do
    Map.replace!(map, key, :redacted)
  end

  defp simple_graph() do
    Journey.new_graph(
      "simple graph #{__MODULE__}",
      "1.0.0",
      [
        input(:user_name),
        compute(
          :greeting,
          [:user_name],
          fn %{user_name: user_name} -> {:ok, "Hello, #{user_name}"} end
        ),
        schedule_recurring(
          :time_to_issue_reminder_schedule_recurring,
          [:greeting],
          fn _ -> {:ok, System.system_time(:second) + 2} end
        ),
        compute(
          :reminder,
          [:greeting, :time_to_issue_reminder_schedule_recurring],
          fn %{greeting: greeting} -> {:ok, "Reminder: #{greeting}"} end
        )
      ]
    )
  end
end
