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
          [:user_name, :user_name],
          fn %{user_name: user_name} ->
            {:ok, "Hello, #{user_name}"}
          end
        ),
        pulse_once(
          :time_to_issue_reminder_pulse,
          [:greeting],
          fn _ -> {:ok, System.system_time(:second) + 1} end
        ),
        compute(
          :reminder,
          [:greeting, :time_to_issue_reminder_pulse],
          fn %{greeting: greeting} ->
            {:ok, "Reminder: #{greeting}"}
          end
        )
      ]
    )
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
        pulse_once(
          :time_to_issue_reminder_pulse,
          [:greeting],
          fn _ -> {:ok, System.system_time(:second) + 1} end
        ),
        compute(
          :reminder,
          [:greeting, :time_to_issue_reminder_pulse],
          fn %{greeting: greeting} ->
            {:ok, "Reminder: #{greeting}"}
          end
        )
      ]
    )
  end
end
