defmodule Journey.Scheduler.Scheduler.RecomputeTest do
  use ExUnit.Case, async: true

  import Journey.Node

  test "basic recompute" do
    graph = simple_graph()

    execution = graph |> Journey.start_execution()

    execution = execution |> Journey.set(:user_name, "Mario")
    execution = execution |> Journey.set(:actual_name, "Bowser")
    {:ok, "Hello, Mario", greeting_rev} = Journey.get(execution, :greeting, wait: :any)

    assert Journey.values(execution) |> redact([:execution_id, :last_updated_at]) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             actual_name: "Bowser",
             execution_id: "...",
             last_updated_at: 1_234_567_890
           }

    # Updating an "upstream" value.
    execution = execution |> Journey.set(:user_name, "Toad")

    # The graph is recomputed.
    {:ok, "Hello, Toad", _} = Journey.get(execution, :greeting, wait: {:newer_than, greeting_rev})

    assert Journey.values(execution) |> redact([:execution_id, :last_updated_at]) == %{
             user_name: "Toad",
             greeting: "Hello, Toad",
             actual_name: "Bowser",
             execution_id: "...",
             last_updated_at: 1_234_567_890
           }
  end

  defp simple_graph() do
    Journey.new_graph(
      "simple graph #{__MODULE__}",
      "1.0.0",
      [
        input(:user_name),
        input(:actual_name),
        compute(
          :greeting,
          [:user_name, :actual_name],
          fn %{user_name: name} ->
            {:ok, "Hello, #{name}"}
          end
        )
      ]
    )
  end
end
