defmodule Journey.Test.Support do
  import Journey.Node

  def create_test_graph1() do
    Journey.new_graph(
      "test graph 1 #{__MODULE__}",
      "1.0.0",
      [
        input(:user_name),
        compute(
          :greeting,
          [:user_name],
          &f_prepend_with_hello/1
        ),
        tick_once(
          :time_to_issue_reminder_schedule,
          [:greeting],
          &f_in_1_second/1
        ),
        compute(
          :reminder,
          [:time_to_issue_reminder_schedule],
          &f_compose_reminder/1
        )
      ]
    )
  end

  defp f_compose_reminder(%{greeting: greeting}) do
    {:ok, "Reminder: #{greeting}"}
  end

  defp f_in_1_second(_) do
    # Why 1 here, not the historical 3: many tests just wait for downstream values to appear
    # and don't care how soon "now" reaches the scheduled time. Tests that *do* care (e.g.
    # `what_am_i_waiting_for/2 sunny day`, which checks the gate state while the schedule
    # is still future) must use their own graphs so they aren't bound to this fixture.
    {:ok, System.system_time(:second) + 1}
  end

  defp f_prepend_with_hello(%{user_name: user_name}) do
    {:ok, "Hello, #{user_name}"}
  end

  def create_test_graph2() do
    Journey.new_graph(
      "test graph 2 #{__MODULE__}",
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
        tick_once(
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
