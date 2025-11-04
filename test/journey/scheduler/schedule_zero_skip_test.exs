defmodule Journey.Scheduler.ScheduleZeroSkipTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Test.Support.Helpers

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  @moduletag timeout: 60_000

  describe "schedule_once with {:ok, 0}" do
    test "returning {:ok, 0} prevents downstream nodes from executing" do
      graph =
        Journey.new_graph(
          "schedule_once zero skip test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            tick_once(
              :schedule_reminder,
              [:user_name],
              fn %{user_name: _name} ->
                # Return 0 to skip scheduling
                {:ok, 0}
              end
            ),
            compute(
              :send_reminder,
              [:user_name, :schedule_reminder],
              fn %{user_name: name} ->
                {:ok, "Reminder for #{name}"}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      execution = Journey.set(execution, :user_name, "Mario")

      # Schedule node should compute and return 0
      assert {:ok, 0, _} = Journey.get(execution, :schedule_reminder, wait: :any)

      # Wait to ensure downstream node doesn't execute
      :timer.sleep(3_000)

      # Downstream node should never execute because schedule returned 0
      assert Journey.get(execution, :send_reminder) == {:error, :not_set}

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "conditional scheduling based on user preference" do
      graph =
        Journey.new_graph(
          "schedule_once conditional test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            input(:wants_reminder),
            tick_once(
              :schedule_reminder,
              [:user_name, :wants_reminder],
              fn %{wants_reminder: wants_reminder} ->
                if wants_reminder do
                  # Schedule for 2 seconds from now
                  {:ok, System.system_time(:second) + 2}
                else
                  # Don't schedule - user opted out
                  {:ok, 0}
                end
              end
            ),
            compute(
              :send_reminder,
              [:user_name, :schedule_reminder],
              fn %{user_name: name} ->
                {:ok, "Reminder for #{name}"}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Test case 1: User doesn't want reminders
      execution = Journey.set(execution, :user_name, "Mario")
      execution = Journey.set(execution, :wants_reminder, false)

      assert {:ok, 0, _} = Journey.get(execution, :schedule_reminder, wait: :any)

      # Wait to ensure downstream node doesn't execute
      :timer.sleep(3_000)
      assert Journey.get(execution, :send_reminder) == {:error, :not_set}

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "changing from 0 to a valid timestamp allows execution" do
      graph =
        Journey.new_graph(
          "schedule_once change from zero test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            input(:wants_reminder),
            tick_once(
              :schedule_reminder,
              [:user_name, :wants_reminder],
              fn %{wants_reminder: wants_reminder} ->
                if wants_reminder do
                  {:ok, System.system_time(:second) + 2}
                else
                  {:ok, 0}
                end
              end
            ),
            compute(
              :send_reminder,
              [:user_name, :schedule_reminder],
              fn %{user_name: name} ->
                {:ok, "Reminder for #{name}"}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Initially user doesn't want reminders
      execution = Journey.set(execution, :user_name, "Mario")
      execution = Journey.set(execution, :wants_reminder, false)

      assert {:ok, 0, _} = Journey.get(execution, :schedule_reminder, wait: :any)
      :timer.sleep(2_000)
      assert Journey.get(execution, :send_reminder) == {:error, :not_set}

      # Now user changes their mind and wants reminders
      execution = Journey.set(execution, :wants_reminder, true)

      # Schedule should now have a real timestamp
      assert {:ok, scheduled_time, _} = Journey.get(execution, :schedule_reminder, wait: :newer)
      assert scheduled_time > 0

      # Downstream node should now execute after the scheduled time
      assert wait_for_value(execution, :send_reminder, "Reminder for Mario", timeout: 10_000)

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end

  describe "schedule_recurring with {:ok, 0}" do
    test "returning {:ok, 0} prevents downstream nodes from executing" do
      graph =
        Journey.new_graph(
          "schedule_recurring zero skip test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            tick_recurring(
              :schedule_reminder,
              [:user_name],
              fn %{user_name: _name} ->
                # Return 0 to skip scheduling
                {:ok, 0}
              end
            ),
            compute(
              :send_reminder,
              [:user_name, :schedule_reminder],
              fn %{user_name: _name} = v ->
                reminder_count = Map.get(v, :send_reminder, 0) + 1
                {:ok, reminder_count}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      execution = Journey.set(execution, :user_name, "Mario")

      # Schedule node should compute and return 0
      assert {:ok, 0, _} = Journey.get(execution, :schedule_reminder, wait: :any)

      # Wait to ensure downstream node doesn't execute
      :timer.sleep(3_000)

      # Downstream node should never execute because schedule returned 0
      assert Journey.get(execution, :send_reminder) == {:error, :not_set}

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "conditional recurring scheduling based on user preference" do
      graph =
        Journey.new_graph(
          "schedule_recurring conditional test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            input(:keep_sending_reminders),
            tick_recurring(
              :schedule_reminder,
              [:user_name, :keep_sending_reminders],
              fn %{keep_sending_reminders: keep_sending} ->
                if keep_sending do
                  # Schedule for 2 seconds from now
                  {:ok, System.system_time(:second) + 2}
                else
                  # Don't schedule - user opted out
                  {:ok, 0}
                end
              end
            ),
            compute(
              :send_reminder,
              [:user_name, :schedule_reminder],
              fn %{user_name: _name} = v ->
                reminder_count = Map.get(v, :send_reminder, 0) + 1
                {:ok, reminder_count}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # User doesn't want recurring reminders
      execution = Journey.set(execution, :user_name, "Mario")
      execution = Journey.set(execution, :keep_sending_reminders, false)

      assert {:ok, 0, _} = Journey.get(execution, :schedule_reminder, wait: :any)

      # Wait to ensure downstream node doesn't execute
      :timer.sleep(3_000)
      assert Journey.get(execution, :send_reminder) == {:error, :not_set}

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "pausing and resuming recurring schedules" do
      graph =
        Journey.new_graph(
          "schedule_recurring pause/resume test #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            input(:keep_sending_reminders),
            tick_recurring(
              :schedule_reminder,
              [:user_name, :keep_sending_reminders],
              fn %{keep_sending_reminders: keep_sending} ->
                if keep_sending do
                  {:ok, System.system_time(:second) + 2}
                else
                  {:ok, 0}
                end
              end
            ),
            compute(
              :send_reminder,
              [:user_name, :schedule_reminder],
              fn %{user_name: _name} = v ->
                reminder_count = Map.get(v, :send_reminder, 0) + 1
                {:ok, reminder_count}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Start with reminders enabled
      execution = Journey.set(execution, :user_name, "Mario")
      execution = Journey.set(execution, :keep_sending_reminders, true)

      # First reminder should be sent
      assert wait_for_value(execution, :send_reminder, 1, timeout: 10_000)

      # Pause reminders
      execution = Journey.set(execution, :keep_sending_reminders, false)
      assert {:ok, 0, _} = Journey.get(execution, :schedule_reminder, wait: :newer)

      # Get current reminder count
      {:ok, count_when_paused, _} = Journey.get(execution, :send_reminder)

      # Wait and verify no new reminders are sent
      :timer.sleep(5_000)
      execution = Journey.load(execution)
      {:ok, count_after_pause, _} = Journey.get(execution, :send_reminder)
      assert count_after_pause == count_when_paused

      # Resume reminders
      execution = Journey.set(execution, :keep_sending_reminders, true)

      # Verify reminders resume
      assert wait_for_value(execution, :send_reminder, count_when_paused + 1, timeout: 10_000)

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end
end
