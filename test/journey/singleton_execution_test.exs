defmodule Journey.SingletonExecutionTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  describe "singleton graph behavior" do
    test "creates execution when none exists" do
      graph = singleton_graph(random_string())

      execution = Journey.ensure_execution(graph)

      assert execution.graph_name == graph.name
      assert execution.graph_version == graph.version
    end

    test "returns existing execution on subsequent calls" do
      graph = singleton_graph(random_string())

      execution1 = Journey.ensure_execution(graph)
      execution2 = Journey.ensure_execution(graph)
      execution3 = Journey.ensure_execution(graph)

      assert execution1.id == execution2.id
      assert execution2.id == execution3.id
    end

    test "maintains state across calls" do
      graph = singleton_graph(random_string())

      execution1 = Journey.ensure_execution(graph)
      _execution1 = Journey.set(execution1, :value, 42)

      execution2 = Journey.ensure_execution(graph)

      assert {:ok, 42, _revision} = Journey.get(execution2, :value)
    end

    test "creates new execution after archiving" do
      graph = singleton_graph(random_string())

      execution1 = Journey.ensure_execution(graph)
      Journey.archive(execution1)

      execution2 = Journey.ensure_execution(graph)

      assert execution1.id != execution2.id
    end

    test "different singleton graphs have different executions" do
      graph1 = singleton_graph(random_string())
      graph2 = singleton_graph(random_string())

      execution1 = Journey.ensure_execution(graph1)
      execution2 = Journey.ensure_execution(graph2)

      assert execution1.id != execution2.id
    end

    test "concurrent calls return the same execution" do
      graph = singleton_graph(random_string())

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            Journey.ensure_execution(graph)
          end)
        end

      executions = Task.await_many(tasks)
      ids = Enum.map(executions, fn e -> e.id end) |> Enum.uniq()

      assert length(ids) == 1
    end
  end

  describe "function validation" do
    test "start_execution raises for singleton graph" do
      graph = singleton_graph(random_string())

      assert_raise ArgumentError, ~r/is a singleton graph/, fn ->
        Journey.start_execution(graph)
      end
    end

    test "ensure_execution raises for regular graph" do
      graph = regular_graph(random_string())

      assert_raise ArgumentError, ~r/is not a singleton graph/, fn ->
        Journey.ensure_execution(graph)
      end
    end

    test "start_execution creates new execution each time for regular graphs" do
      graph = regular_graph(random_string())

      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)

      assert execution1.id != execution2.id
    end
  end

  describe "start/1 alias" do
    test "works with regular graphs" do
      graph = regular_graph(random_string())

      execution1 = Journey.start(graph)
      execution2 = Journey.start(graph)

      assert execution1.id != execution2.id
    end

    test "raises for singleton graphs" do
      graph = singleton_graph(random_string())

      assert_raise ArgumentError, ~r/is a singleton graph/, fn ->
        Journey.start(graph)
      end
    end
  end

  defp singleton_graph(test_id) do
    Journey.new_graph(
      "singleton test #{test_id}",
      "1.0.0",
      [input(:value)],
      singleton: true
    )
  end

  defp regular_graph(test_id) do
    Journey.new_graph(
      "regular test #{test_id}",
      "1.0.0",
      [input(:value)]
    )
  end
end
