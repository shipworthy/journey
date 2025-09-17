defmodule Journey.NewGraphHashTest do
  use ExUnit.Case, async: true

  import Journey.Node

  describe "graph hash computation" do
    test "generates consistent hash for same graph structure" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(:c, [:a, :b], fn _ -> {:ok, "result"} end)
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(:c, [:a, :b], fn _ -> {:ok, "result"} end)
          ]
        )

      assert graph1.hash == graph2.hash
    end

    test "generates different hash when nodes are added" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b)
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            input(:c)
          ]
        )

      assert graph1.hash != graph2.hash
    end

    test "generates different hash when node types change" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a)
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            compute(:a, [], fn _ -> {:ok, "value"} end)
          ]
        )

      assert graph1.hash != graph2.hash
    end

    test "generates same hash when compute dependencies change" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(:c, [:a], fn _ -> {:ok, "result"} end)
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(:c, [:a, :b], fn _ -> {:ok, "result"} end)
          ]
        )

      assert graph1.hash == graph2.hash
    end

    test "hash is consistent regardless of node order" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:b),
            input(:a),
            compute(:c, [:a, :b], fn _ -> {:ok, "result"} end)
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            compute(:c, [:a, :b], fn _ -> {:ok, "result"} end),
            input(:b),
            input(:a)
          ]
        )

      assert graph1.hash == graph2.hash
    end
  end

  describe "hash propagation to executions" do
    test "execution receives graph hash when created" do
      graph =
        Journey.new_graph(
          "test_graph_for_execution",
          "v1",
          [
            input(:a),
            input(:b)
          ]
        )

      execution = Journey.start_execution(graph)

      assert execution.graph_hash == graph.hash
      assert is_binary(execution.graph_hash)
      # SHA256 in hex
      assert String.length(execution.graph_hash) == 64
    end

    test "different graphs produce executions with different hashes" do
      graph1 =
        Journey.new_graph(
          "test_graph_1",
          "v1",
          [input(:a)]
        )

      graph2 =
        Journey.new_graph(
          "test_graph_2",
          "v1",
          [input(:a), input(:b)]
        )

      execution1 = Journey.start_execution(graph1)
      execution2 = Journey.start_execution(graph2)

      assert execution1.graph_hash != execution2.graph_hash
    end
  end
end
