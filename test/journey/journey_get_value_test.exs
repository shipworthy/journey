defmodule Journey.JourneyGetValueTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "get_value" do
    test "sunny day, input, not set, non-blocking" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert Journey.get_value(execution, :first_name) == {:error, :not_set}
    end

    test "sunny day, input, set, non-blocking" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
    end

    test "sunny day, computation, set, blocking" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :greeting, wait_any: true) == {:ok, "Hello, Mario"}
    end

    test "no such node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert_raise RuntimeError,
                   "':no_such_node' is not a known node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid node names: [:execution_id, :first_name, :greeting, :last_updated_at].",
                   fn ->
                     Journey.get_value(execution, :no_such_node, wait_any: true) == {:ok, "Hello, Mario"}
                   end
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
