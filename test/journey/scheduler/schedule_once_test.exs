defmodule Journey.Scheduler.Scheduler.ScheduleOnceTest do
  use ExUnit.Case, async: true

  @tag :skip

  import Journey.Node

  alias Journey.Scheduler.BackgroundSweeps

  test "basic pulse" do
    graph = simple_graph()
    execution = graph |> Journey.start_execution()

    start_time = System.system_time(:second)

    background_sweeps_task = BackgroundSweeps.start_background_sweeps_in_test(execution.id)

    execution = execution |> Journey.set_value(:user_name, "Mario")

    assert Journey.get_value(execution, :greeting, wait_any: true) == {:ok, "Hello, Mario"}

    assert Journey.get_value(execution, :reminder, wait_any: 20_000) == {:ok, "Reminder: Hello, Mario"}

    assert Journey.values(execution)
           |> redact([:time_to_issue_reminder_schedule, :execution_id, :last_updated_at]) ==
             %{
               greeting: "Hello, Mario",
               reminder: "Reminder: Hello, Mario",
               user_name: "Mario",
               time_to_issue_reminder_schedule: 1_234_567_890,
               execution_id: "...",
               last_updated_at: 1_234_567_890
             }

    assert Journey.get_value(execution, :reminder, wait_any: 20_000) == {:ok, "Reminder: Hello, Mario"}

    assert Journey.values(execution)
           |> redact([:time_to_issue_reminder_schedule, :execution_id, :last_updated_at]) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             reminder: "Reminder: Hello, Mario",
             time_to_issue_reminder_schedule: 1_234_567_890,
             execution_id: "...",
             last_updated_at: 1_234_567_890
           }

    assert Journey.get_value(execution, :reminder, wait_any: 20_000) == {:ok, "Reminder: Hello, Mario"}

    assert Journey.values(execution)
           |> redact([:time_to_issue_reminder_schedule, :execution_id, :last_updated_at]) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             reminder: "Reminder: Hello, Mario",
             time_to_issue_reminder_schedule: 1_234_567_890,
             execution_id: "...",
             last_updated_at: 1_234_567_890
           }

    end_time = System.system_time(:second)

    assert end_time - start_time >= 10

    BackgroundSweeps.stop_background_sweeps_in_test(background_sweeps_task)

    # TODO: add a recompute (modify user_name) and watch the change propagate (think through use cases).
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
          fn _ -> {:ok, System.system_time(:second) + 10} end
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
