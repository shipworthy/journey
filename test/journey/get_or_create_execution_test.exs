defmodule Journey.GetOrCreateExecutionTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  describe "get_or_create_execution/1" do
    test "creates execution when none exists" do
      graph = basic_graph(random_string())

      execution = Journey.get_or_create_execution(graph)

      assert execution.graph_name == graph.name
      assert execution.graph_version == graph.version
    end

    test "returns existing execution on subsequent calls" do
      graph = basic_graph(random_string())

      execution1 = Journey.get_or_create_execution(graph)
      execution2 = Journey.get_or_create_execution(graph)
      execution3 = Journey.get_or_create_execution(graph)

      assert execution1.id == execution2.id
      assert execution2.id == execution3.id
    end

    test "maintains state across calls" do
      graph = basic_graph(random_string())

      execution1 = Journey.get_or_create_execution(graph)
      _execution1 = Journey.set(execution1, :value, 42)

      execution2 = Journey.get_or_create_execution(graph)

      assert {:ok, 42, _revision} = Journey.get(execution2, :value)
    end

    test "creates new execution after archiving" do
      graph = basic_graph(random_string())

      execution1 = Journey.get_or_create_execution(graph)
      Journey.archive(execution1)

      execution2 = Journey.get_or_create_execution(graph)

      assert execution1.id != execution2.id
    end

    test "different graphs have different executions" do
      graph1 = basic_graph(random_string())
      graph2 = basic_graph(random_string())

      execution1 = Journey.get_or_create_execution(graph1)
      execution2 = Journey.get_or_create_execution(graph2)

      assert execution1.id != execution2.id
    end

    test "concurrent calls return the same execution" do
      graph = basic_graph(random_string())

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            Journey.get_or_create_execution(graph)
          end)
        end

      executions = Task.await_many(tasks)
      ids = Enum.map(executions, & &1.id) |> Enum.uniq()

      assert length(ids) == 1
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "get_or_create test #{test_id}",
      "1.0.0",
      [input(:value)]
    )
  end
end
