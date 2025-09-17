defmodule Journey.JourneyLoadTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "load" do
    test "sunny day" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      loaded_by_id = Journey.load(execution.id)
      loaded_by_execution = Journey.load(execution)

      assert execution == loaded_by_id
      assert execution == loaded_by_execution
    end

    test "nil" do
      assert nil == Journey.load(nil)
    end

    test "no such execution" do
      _execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert nil == Journey.load("no_such_execution_id")
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "basic graph, greetings #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:first_name),
        compute(
          :greeting,
          unblocked_when({:first_name, &provided?/1}),
          fn %{first_name: first_name} ->
            {:ok, "Hello, #{first_name}"}
          end
        )
      ]
    )
  end
end
