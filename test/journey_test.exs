defmodule JourneyTest do
  use ExUnit.Case, async: true
  doctest Journey

  import Journey.Node

  describe "get_value" do
    test "sunny day, input, not set, non-blocking" do
      execution =
        basic_graph()
        |> Journey.start_execution()

      assert Journey.get_value(execution, :first_name) == {:error, :not_set}
    end

    test "sunny day, input, set, non-blocking" do
      execution =
        basic_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
    end

    test "sunny day, computation, set, blocking" do
      execution =
        basic_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Mario"}
    end

    test "no such node" do
      execution =
        basic_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert_raise RuntimeError,
                   "':no_such_node' is not a known node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid node names: [:first_name, :greeting].",
                   fn ->
                     Journey.get_value(execution, :no_such_node, wait: true) == {:ok, "Hello, Mario"}
                   end
    end
  end

  describe "set_value" do
    test "sunny day" do
      execution =
        basic_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
    end

    test "unknown node" do
      execution =
        basic_graph()
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':last_name' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:first_name].",
                   fn ->
                     Journey.set_value(execution, :last_name, "Bowser")
                   end
    end

    test "not an input node" do
      execution =
        basic_graph()
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':greeting' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:first_name].",
                   fn ->
                     Journey.set_value(execution, :greeting, "Hello!")
                   end
    end
  end

  defp basic_graph() do
    Journey.new_graph(
      "basic graph, greetings #{__MODULE__}",
      "1.0.0",
      [
        input(:first_name),
        compute(
          :greeting,
          [:first_name],
          fn %{first_name: first_name} ->
            {:ok, "Hello, #{first_name}"}
          end
        )
      ]
    )
  end
end
