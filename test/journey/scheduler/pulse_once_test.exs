defmodule Journey.Scheduler.Scheduler.PulseOnceTest do
  use ExUnit.Case, async: true

  import Journey.Node

  alias Journey.Scheduler.BackgroundSweeps.Scheduled

  #   @tag :skip
  test "basic pulse" do
    graph = simple_graph()
    execution = graph |> Journey.start_execution()

    execution = execution |> Journey.set_value(:user_name, "Mario")
    Scheduled.sweep(execution.id)

    assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Mario"}

    Process.sleep(2000)
    Scheduled.sweep(execution.id)
    Process.sleep(2000)

    assert Journey.get_value(execution, :reminder, wait: 10_000) == {:ok, "Reminder: Hello, Mario"}

    assert Journey.values(execution) |> redact(:time_to_issue_reminder_schedule) == %{
             greeting: "Hello, Mario",
             reminder: "Reminder: Hello, Mario",
             user_name: "Mario",
             time_to_issue_reminder_schedule: :redacted
           }

    Scheduled.sweep(execution.id)
    assert Journey.get_value(execution, :reminder, wait: true) == {:ok, "Reminder: Hello, Mario"}

    assert Journey.values(execution) |> redact(:time_to_issue_reminder_schedule) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             reminder: "Reminder: Hello, Mario",
             time_to_issue_reminder_schedule: :redacted
           }

    assert Journey.get_value(execution, :reminder, wait: true) == {:ok, "Reminder: Hello, Mario"}

    assert Journey.values(execution) |> redact(:time_to_issue_reminder_schedule) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             reminder: "Reminder: Hello, Mario",
             time_to_issue_reminder_schedule: :redacted
           }

    # TODO: add a recompute (modify user_name) and watch the change propagate (think through use cases).
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
          [:user_name, :user_name],
          fn %{user_name: user_name} ->
            {:ok, "Hello, #{user_name}"}
          end
        ),
        schedule_once(
          :time_to_issue_reminder_schedule,
          [:greeting],
          fn _ -> {:ok, System.system_time(:second) + 1} end
        ),
        compute(
          :reminder,
          [:greeting, :time_to_issue_reminder_schedule],
          fn %{greeting: greeting} ->
            {:ok, "Reminder: #{greeting}"}
          end
        )
      ]
    )
  end
end
