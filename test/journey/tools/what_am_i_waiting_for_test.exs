defmodule Journey.Tools.WhatAmIWaitingForTest do
  use ExUnit.Case, async: true

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  describe "what_am_i_waiting_for/2" do
    test "sunny day" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      result = Journey.Tools.what_am_i_waiting_for(execution.id, :reminder)
      assert result == "🛑 :greeting | &provided?/1\n🛑 :time_to_issue_reminder_schedule | &provided?/1"

      execution = Journey.set(execution, :user_name, "Bowser")

      {:ok, "Hello, Bowser"} = Journey.get_value(execution, :greeting, wait_any: true)
      {:ok, _reminder_scheduled} = Journey.get_value(execution, :time_to_issue_reminder_schedule, wait_any: true)

      result = Journey.Tools.what_am_i_waiting_for(execution.id, :reminder)
      assert result == "✅ :greeting | &provided?/1 | rev 3\n🛑 :time_to_issue_reminder_schedule | &provided?/1"

      background_sweeps_task = start_background_sweeps_in_test(execution.id)
      {:ok, _reminder_value} = Journey.get_value(execution, :reminder, wait_any: true)

      result = Journey.Tools.what_am_i_waiting_for(execution.id, :reminder)
      assert result == "✅ :greeting | &provided?/1 | rev 3\n✅ :time_to_issue_reminder_schedule | &provided?/1 | rev 5\n"

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end
end
