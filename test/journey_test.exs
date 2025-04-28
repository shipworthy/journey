defmodule JourneyTest do
  use ExUnit.Case, async: true
  doctest Journey

  import Journey.Node

  # TODO: split this into multiple modules that can be run in parallel

  describe "load" do
    test "sunny day" do
      execution =
        basic_graph()
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
        basic_graph()
        |> Journey.start_execution()

      assert nil == Journey.load("no_such_execution_id")
    end
  end

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

  describe "list_executions" do
    test "sunny day, by graph name" do
      execution =
        basic_graph()
        |> Journey.start_execution()

      listed_executions = Journey.list_executions(graph_name: execution.graph_name)

      for le <- listed_executions do
        # Making sure that values and computations are loaded.
        assert Enum.count(le.values) == 2
        assert Enum.count(le.computations) == 1
      end

      assert execution.id in (listed_executions |> Enum.map(& &1.id))
    end

    test "sunny day, sort by inserted_at (which is updated after a set_value)" do
      execution_ids =
        Enum.map(1..3, fn _ ->
          basic_graph()
          |> Journey.start_execution()
          |> Map.get(:id)
          |> tap(fn _ -> Process.sleep(1_000) end)
        end)

      # Updating the first execution should put it at the back.
      [first_id | remaining_ids] = execution_ids

      updated_execution =
        first_id
        |> Journey.load()
        |> Journey.set_value(:first_name, "Mario")

      expected_order = remaining_ids ++ [first_id]
      {:ok, "Hello, Mario"} = Journey.get_value(updated_execution, :greeting, wait: true)

      listed_execution_ids =
        Journey.list_executions(graph_name: basic_graph().name, order_by_fields: [:updated_at])
        |> Enum.map(& &1.id)
        |> Enum.filter(fn id -> id in execution_ids end)

      assert expected_order == listed_execution_ids
    end

    test "no executions" do
      assert Journey.list_executions(graph_name: "no_such_graph") == []
    end

    test "unexpected option" do
      assert_raise ArgumentError, "Unknown options: [:graph]. Known options: [:graph_name, :order_by_fields].", fn ->
        Journey.list_executions(graph: "no_such_graph")
      end
    end
  end

  describe "set_value |" do
    test "sunny day" do
      execution =
        basic_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
    end

    test "sunny day, updated_at timestamps get updated" do
      execution =
        basic_graph()
        |> Journey.start_execution()

      Process.sleep(1200)

      execution =
        execution
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
      {:ok, greeting} = Journey.get_value(execution, :greeting, wait: true)
      assert greeting == "Hello, Mario"

      execution = execution |> Journey.load()

      first_name_node = value_node(execution, :first_name)
      assert first_name_node.inserted_at < first_name_node.updated_at

      greeting_value_node = value_node(execution, :greeting)
      assert greeting_value_node.inserted_at < greeting_value_node.updated_at

      compute_node = computation_node(execution, :greeting)
      assert compute_node.inserted_at < compute_node.updated_at

      assert execution.inserted_at < execution.updated_at
    end

    def value_node(execution, node_name) do
      execution.values |> Enum.find(fn x -> x.node_name == node_name end)
    end

    def computation_node(execution, node_name) do
      execution.computations |> Enum.find(fn x -> x.node_name == node_name end)
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
