defmodule Journey.JourneyTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  # TODO: split this into multiple modules that can be run in parallel

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

      assert Journey.get_value(execution, :greeting, wait: true) == {:ok, "Hello, Mario"}
    end

    test "no such node" do
      execution =
        basic_graph(random_string())
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
    test "sunny day, limit / offset" do
      graph = basic_graph(random_string())
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set_value(:first_name, i)

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 20)
      assert Enum.count(listed_executions) == 20

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 11, offset: 30)
      assert Enum.count(listed_executions) == 11

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 20, offset: 90)
      assert Enum.count(listed_executions) == 10
    end

    test "sunny day, filer by value" do
      graph = basic_graph(random_string())
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set_value(:first_name, i)

      listed_executions = Journey.list_executions(graph_name: graph.name)
      assert Enum.count(listed_executions) == 100

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :lt, 20}])
      assert Enum.count(some_executions) == 19

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :lte, 20}])
      assert Enum.count(some_executions) == 20

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :eq, 50}])
      assert Enum.count(some_executions) == 1

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :neq, 50}])
      assert Enum.count(some_executions) == 99

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :gt, 60}])
      assert Enum.count(some_executions) == 40

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :gte, 60}])
      assert Enum.count(some_executions) == 41

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :in, [20, 22]}])
      assert Enum.count(some_executions) == 2

      neq = fn node_value, val -> node_value != val end
      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, neq, 1}])
      assert Enum.count(some_executions) == 99

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :is_not_nil}])
      assert Enum.count(some_executions) == 100

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :is_nil}])
      assert some_executions == []

      is_one = fn node_value -> node_value == 1 end
      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, is_one}])
      assert Enum.count(some_executions) == 1
    end

    test "sunny day, by graph name" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      Process.sleep(1_000)
      listed_executions = Journey.list_executions(graph_name: execution.graph_name)

      for le <- listed_executions do
        # Making sure that values and computations are loaded.
        assert Enum.count(le.values) == 2
        assert Enum.count(le.computations) == 1, "#{inspect(le.computations)}"
      end

      assert execution.id in (listed_executions |> Enum.map(& &1.id))
    end

    test "sunny day, sort by inserted_at (which is updated after a set_value)" do
      test_id = random_string()

      execution_ids =
        Enum.map(1..3, fn _ ->
          basic_graph(test_id)
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
        Journey.list_executions(graph_name: basic_graph(test_id).name, order_by_execution_fields: [:updated_at])
        |> Enum.map(& &1.id)
        |> Enum.filter(fn id -> id in execution_ids end)

      assert expected_order == listed_execution_ids
    end

    test "no executions" do
      assert Journey.list_executions(graph_name: "no_such_graph") == []
    end

    test "unexpected option" do
      assert_raise ArgumentError,
                   "Unknown options: [:graph]. Known options: [:graph_name, :limit, :offset, :order_by_execution_fields, :value_filters].",
                   fn ->
                     Journey.list_executions(graph: "no_such_graph")
                   end
    end
  end

  describe "set_value |" do
    test "sunny day" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
    end

    test "setting value to the same value" do
      execution_v1 =
        basic_graph(random_string())
        |> Journey.start_execution()

      execution_v1 |> Journey.set_value(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution_v1, :first_name, wait: true)
      {:ok, "Hello, Mario"} = Journey.get_value(execution_v1, :greeting, wait: true)
      execution_after_first_set = execution_v1 |> Journey.load()

      assert execution_after_first_set.revision > execution_v1.revision

      execution_v1 |> Journey.set_value(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution_v1, :first_name, wait: true)
      execution_after_second_set = execution_v1 |> Journey.load()
      assert execution_after_second_set.revision == execution_after_first_set.revision
    end

    test "setting value to a new value" do
      execution_v1 =
        basic_graph(random_string())
        |> Journey.start_execution()

      execution_v2 = execution_v1 |> Journey.set_value(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution_v1, :first_name, wait: true)
      {:ok, "Hello, Mario"} = Journey.get_value(execution_v1, :greeting, wait: true)
      assert execution_v2.revision > execution_v1.revision

      execution_v3 = execution_v1 |> Journey.set_value(:first_name, "Luigi")
      # TODO: add semantics for waiting for a computation to finish.
      Process.sleep(500)
      {:ok, "Luigi"} = Journey.get_value(execution_v3, :first_name, wait: true)
      {:ok, "Hello, Luigi"} = Journey.get_value(execution_v3, :greeting, wait: true)
      assert execution_v3.revision > execution_v2.revision
    end

    test "sunny day, updated_at timestamps get updated" do
      execution =
        basic_graph(random_string())
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
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':last_name' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:first_name].",
                   fn ->
                     Journey.set_value(execution, :last_name, "Bowser")
                   end
    end

    test "not an input node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':greeting' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:first_name].",
                   fn ->
                     Journey.set_value(execution, :greeting, "Hello!")
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
