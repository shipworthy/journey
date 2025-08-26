defmodule Journey.GraphHashTest do
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

    test "generates different hash when compute dependencies change" do
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

      assert graph1.hash != graph2.hash
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

  describe "complex gated_by conditions" do
    test "hash differs with :or vs simple list dependencies" do
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
            compute(
              :c,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :or,
                [{:a, &Journey.Node.Conditions.provided?/1}, {:b, &Journey.Node.Conditions.provided?/1}]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      assert graph1.hash != graph2.hash
    end

    test "hash differs with :and conditions" do
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
            compute(
              :c,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :and,
                [{:a, &Journey.Node.Conditions.provided?/1}, {:b, &Journey.Node.Conditions.provided?/1}]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      # :and with predicates is different from simple list
      assert graph1.hash != graph2.hash
    end

    test "hash differs with :not conditions" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(
              :c,
              Journey.Node.UpstreamDependencies.unblocked_when({:a, &Journey.Node.Conditions.provided?/1}),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(
              :c,
              Journey.Node.UpstreamDependencies.unblocked_when({:not, {:a, &Journey.Node.Conditions.provided?/1}}),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      assert graph1.hash != graph2.hash
    end

    test "hash differs with nested :and/:or conditions" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:g1_a),
            input(:g1_b),
            input(:g2_a),
            input(:g2_b),
            compute(
              :result,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :and,
                [
                  {:or, [{:g1_a, &Journey.Node.Conditions.provided?/1}, {:g1_b, &Journey.Node.Conditions.provided?/1}]},
                  {:or, [{:g2_a, &Journey.Node.Conditions.provided?/1}, {:g2_b, &Journey.Node.Conditions.provided?/1}]}
                ]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:g1_a),
            input(:g1_b),
            input(:g2_a),
            input(:g2_b),
            compute(
              :result,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :or,
                [
                  {:and,
                   [{:g1_a, &Journey.Node.Conditions.provided?/1}, {:g1_b, &Journey.Node.Conditions.provided?/1}]},
                  {:and, [{:g2_a, &Journey.Node.Conditions.provided?/1}, {:g2_b, &Journey.Node.Conditions.provided?/1}]}
                ]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      # Different nesting structure should produce different hash
      assert graph1.hash != graph2.hash
    end

    test "hash differs with deeply nested conditions" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            input(:c),
            input(:d),
            compute(
              :result,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :and,
                [
                  {:or, [{:a, &Journey.Node.Conditions.provided?/1}, {:b, &Journey.Node.Conditions.provided?/1}]},
                  {:and, [{:c, &Journey.Node.Conditions.provided?/1}, {:d, &Journey.Node.Conditions.provided?/1}]}
                ]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            input(:c),
            input(:d),
            compute(
              :result,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :or,
                [
                  {:and, [{:a, &Journey.Node.Conditions.provided?/1}, {:c, &Journey.Node.Conditions.provided?/1}]},
                  {:and, [{:b, &Journey.Node.Conditions.provided?/1}, {:d, &Journey.Node.Conditions.provided?/1}]}
                ]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      # Different nested structure should produce different hash
      assert graph1.hash != graph2.hash
    end

    test "hash differs when condition order changes" do
      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(
              :c,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :or,
                [{:a, &Journey.Node.Conditions.provided?/1}, {:b, &Journey.Node.Conditions.provided?/1}]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            input(:b),
            compute(
              :c,
              Journey.Node.UpstreamDependencies.unblocked_when({
                :or,
                [{:b, &Journey.Node.Conditions.provided?/1}, {:a, &Journey.Node.Conditions.provided?/1}]
              }),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      # Order of conditions is part of the graph structure, so hashes differ
      assert graph1.hash != graph2.hash
    end

    test "hash differs with different predicates" do
      defmodule TestPredicates do
        def always_true(_), do: true
        def always_false(_), do: false
      end

      graph1 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            compute(
              :b,
              Journey.Node.UpstreamDependencies.unblocked_when({:a, &Journey.Node.Conditions.provided?/1}),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      graph2 =
        Journey.new_graph(
          "test_graph",
          "v1",
          [
            input(:a),
            compute(
              :b,
              Journey.Node.UpstreamDependencies.unblocked_when({:a, &TestPredicates.always_true/1}),
              fn _ -> {:ok, "result"} end
            )
          ]
        )

      # Different predicates should produce different hashes
      assert graph1.hash != graph2.hash
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
