defmodule Journey.Scheduler.Scheduler.PulseRecurringTest do
  use ExUnit.Case, async: true

  import Journey.Node
  alias Journey.Scheduler.BackgroundSweeps

  @tag :skip
  test "basic schedule_recurring flow" do
    # TODO: implement
    graph = simple_graph()
    execution = graph |> Journey.start_execution()
    background_sweeps_task = BackgroundSweeps.start_background_sweeps_in_test(execution.id)

    execution = execution |> Journey.set_value(:user_name, "Mario")

    assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Mario"}

    Process.sleep(1000)
    {:ok, _time} = Journey.get_value(execution, :time_to_issue_reminder_schedule_recurring, wait: true)

    assert {:ok, "Reminder: Hello, Mario"} = Journey.get_value(execution, :reminder, wait: true)

    assert Journey.values(execution) |> redact([:time_to_issue_reminder_schedule_recurring, :execution_id]) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             reminder: "Reminder: Hello, Mario",
             time_to_issue_reminder_schedule_recurring: 1_234_567_890,
             execution_id: "..."
           }

    BackgroundSweeps.stop_background_sweeps_in_test(background_sweeps_task)
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
