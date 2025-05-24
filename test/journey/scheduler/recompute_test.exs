defmodule Journey.Scheduler.Scheduler.RecomputeTest do
  use ExUnit.Case, async: true

  import Journey.Node

  test "basic recompute" do
    graph = simple_graph()

    execution = graph |> Journey.start_execution()

    execution = execution |> Journey.set_value(:user_name, "Mario")
    execution = execution |> Journey.set_value(:actual_name, "Bowser")
    assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Mario"}

    assert Journey.values(execution) |> Map.update!(:execution_id, fn _ -> "EXEC..." end) == %{
             greeting: "Hello, Mario",
             user_name: "Mario",
             actual_name: "Bowser",
             execution_id: "EXEC..."
           }

    # Updating an "upstream" value.
    execution = execution |> Journey.set_value(:user_name, "Luigi")

    # TODO: implement immediate get_value -- so it waits until there are no incomplete computations for this node... is that really needed, though?
    # assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Luigi"}

    Process.sleep(1_000)
    # The graph is recomputed.
    assert Journey.values(execution) |> Map.update!(:execution_id, fn _ -> "EXEC..." end) == %{
             user_name: "Luigi",
             greeting: "Hello, Luigi",
             actual_name: "Bowser",
             execution_id: "EXEC..."
           }

    assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Luigi"}
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
