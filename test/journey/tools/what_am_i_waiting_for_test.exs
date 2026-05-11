defmodule Journey.Tools.WhatAmIWaitingForTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  # The "sunny day" test below depends on `provided?/1` for schedule nodes returning
  # `false` while `now < scheduled_time`. With a 30-second schedule there is no chance
  # the test wall-clock passes the scheduled time before the gate-state assertion runs,
  # so the test is timing-independent for those assertions. The final `✅` assertion is
  # the only one that requires `now >= scheduled_time`; we wait for it via sweeps.
  defp graph_with_30s_schedule() do
    Journey.new_graph(
      "what_am_i_waiting_for sunny day #{random_string()}",
      "1.0.0",
      [
        input(:user_name),
        compute(:greeting, [:user_name], fn %{user_name: name} -> {:ok, "Hello, #{name}"} end),
        tick_once(
          :time_to_issue_reminder_schedule,
          [:greeting],
          fn _ -> {:ok, System.system_time(:second) + 30} end
        ),
        compute(
          :reminder,
          [:time_to_issue_reminder_schedule],
          fn %{greeting: greeting} -> {:ok, "Reminder: #{greeting}"} end
        )
      ]
    )
  end

  describe "what_am_i_waiting_for/2" do
    test "sunny day" do
      graph = graph_with_30s_schedule()
      execution = Journey.start_execution(graph)

      result = Journey.Tools.what_am_i_waiting_for(execution.id, :reminder)
      assert result == "🛑 :time_to_issue_reminder_schedule | &provided?/1"

      execution = Journey.set(execution, :user_name, "Bowser")

      {:ok, "Hello, Bowser"} = Journey.get_value(execution, :greeting, wait_any: true)

      {:ok, _reminder_scheduled} =
        Journey.get_value(execution, :time_to_issue_reminder_schedule, wait_any: true)

      # Schedule is in the future (~30s away), so `provided?` returns false.
      result = Journey.Tools.what_am_i_waiting_for(execution.id, :reminder)
      assert result == "🛑 :time_to_issue_reminder_schedule | &provided?/1"

      # Force the schedule's stored timestamp to "now" so we don't have to wait 30 seconds
      # for the gate to flip. This exercises the same `provided?` branch as a naturally
      # elapsed schedule.
      now = System.system_time(:second)
      import Ecto.Query

      from(v in Journey.Persistence.Schema.Execution.Value,
        where: v.execution_id == ^execution.id and v.node_name == "time_to_issue_reminder_schedule"
      )
      |> Journey.Repo.update_all(set: [node_value: now])

      background_sweeps_task = start_background_sweeps_in_test(execution.id)
      {:ok, _reminder_value} = Journey.get_value(execution, :reminder, wait_any: true)

      result = Journey.Tools.what_am_i_waiting_for(execution.id, :reminder)
      assert result == "✅ :time_to_issue_reminder_schedule | &provided?/1 | rev 5\n"

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end
end
