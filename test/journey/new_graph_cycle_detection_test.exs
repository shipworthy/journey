defmodule Journey.NewGraphCycleDetectionTest do
  use ExUnit.Case, async: true

  import Journey.Node

  describe "circular dependency detection" do
    test "simple two-node cycle" do
      assert_raise RuntimeError, "Circular dependency detected in graph 'simple cycle test': :a → :b → :a", fn ->
        Journey.new_graph(
          "simple cycle test",
          "v1.0.0",
          [
            compute(:a, [:b], fn %{b: b} -> {:ok, "a depends on #{b}"} end),
            compute(:b, [:a], fn %{a: a} -> {:ok, "b depends on #{a}"} end)
          ]
        )
      end
    end

    test "self-referencing cycle" do
      assert_raise RuntimeError, "Circular dependency detected in graph 'self cycle test': :a → :a", fn ->
        Journey.new_graph(
          "self cycle test",
          "v1.0.0",
          [
            compute(:a, [:a], fn %{a: a} -> {:ok, "self reference #{a}"} end)
          ]
        )
      end
    end

    test "three-node cycle" do
      assert_raise RuntimeError, "Circular dependency detected in graph 'three node cycle': :a → :c → :b → :a", fn ->
        Journey.new_graph(
          "three node cycle",
          "v1.0.0",
          [
            compute(:a, [:c], fn %{c: c} -> {:ok, "a depends on #{c}"} end),
            compute(:b, [:a], fn %{a: a} -> {:ok, "b depends on #{a}"} end),
            compute(:c, [:b], fn %{b: b} -> {:ok, "c depends on #{b}"} end)
          ]
        )
      end
    end

    test "valid DAG with multiple dependencies - no cycle" do
      graph =
        Journey.new_graph(
          "valid dag test",
          "v1.0.0",
          [
            input(:x),
            input(:y),
            compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end),
            compute(:product, [:x, :y], fn %{x: x, y: y} -> {:ok, x * y} end),
            compute(:final, [:sum, :product], fn %{sum: s, product: p} -> {:ok, s + p} end)
          ]
        )

      assert graph.name == "valid dag test"
      # 5 defined + 2 auto-added
      assert length(graph.nodes) == 7
    end

    test "complex cycle with conditional dependencies" do
      import Journey.Node.Conditions
      import Journey.Node.UpstreamDependencies

      assert_raise RuntimeError,
                   "Circular dependency detected in graph 'conditional cycle test': :a → :b → :a",
                   fn ->
                     Journey.new_graph(
                       "conditional cycle test",
                       "v1.0.0",
                       [
                         input(:trigger),
                         compute(
                           :a,
                           unblocked_when({:and, [{:trigger, &provided?/1}, {:b, &provided?/1}]}),
                           fn %{trigger: t, b: b} -> {:ok, "a: #{t}, #{b}"} end
                         ),
                         compute(:b, [:a], fn %{a: a} -> {:ok, "b depends on #{a}"} end)
                       ]
                     )
                   end
    end

    test "four-node cycle buried in larger graph" do
      assert_raise RuntimeError,
                   "Circular dependency detected in graph 'deep cycle test': :a → :d → :c → :b → :a",
                   fn ->
                     Journey.new_graph(
                       "deep cycle test",
                       "v1.0.0",
                       [
                         input(:start),
                         compute(:step1, [:start], fn %{start: s} -> {:ok, "step1: #{s}"} end),
                         compute(:step2, [:step1], fn %{step1: s} -> {:ok, "step2: #{s}"} end),
                         # Create a cycle: a -> b -> c -> d and separately d -> a (which creates a->b->c->d->a cycle)
                         compute(:a, [:step2, :d], fn %{step2: s, d: d} -> {:ok, "a: #{s}, #{d}"} end),
                         compute(:b, [:a], fn %{a: a} -> {:ok, "b: #{a}"} end),
                         compute(:c, [:b], fn %{b: b} -> {:ok, "c: #{b}"} end),
                         # Creates cycle: a->b->c->d, and a depends on d
                         compute(:d, [:c], fn %{c: c} -> {:ok, "d: #{c}"} end)
                       ]
                     )
                   end
    end

    test "mutate node does not create dependency cycle" do
      # This should NOT be a cycle - mutate relationships are write dependencies, not read dependencies
      graph =
        Journey.new_graph(
          "mutate no cycle test",
          "v1.0.0",
          [
            input(:data),
            compute(:processed, [:data], fn %{data: d} -> {:ok, "processed #{d}"} end),
            mutate(
              :update_data,
              [:processed],
              fn %{data: d} -> {:ok, "updated #{d}"} end,
              mutates: :data
            )
          ]
        )

      assert graph.name == "mutate no cycle test"
      # 3 defined + 2 auto-added
      assert length(graph.nodes) == 5
    end

    test "mutate node creating actual dependency cycle" do
      assert_raise RuntimeError,
                   "Circular dependency detected in graph 'mutate real cycle test': :a → :c → :b → :a",
                   fn ->
                     Journey.new_graph(
                       "mutate real cycle test",
                       "v1.0.0",
                       [
                         input(:seed),
                         compute(:a, [:seed, :c], fn %{seed: s, c: c} -> {:ok, "a: #{s}, #{c}"} end),
                         compute(:b, [:a], fn %{a: a} -> {:ok, "b: #{a}"} end),
                         compute(:c, [:b], fn %{b: b} -> {:ok, "c: #{b}"} end),
                         # The mutate itself doesn't create the cycle - the dependency does
                         mutate(:update_seed, [:c], fn %{seed: s} -> {:ok, "updated #{s}"} end, mutates: :seed)
                       ]
                     )
                   end
    end

    test "schedule nodes with cycles are detected" do
      assert_raise RuntimeError,
                   "Circular dependency detected in graph 'schedule cycle test': :result → :scheduler → :result",
                   fn ->
                     Journey.new_graph(
                       "schedule cycle test",
                       "v1.0.0",
                       [
                         input(:trigger),
                         schedule_once(:scheduler, [:trigger, :result], fn %{trigger: t, result: r} ->
                           {:ok, "scheduled: #{t}, #{r}"}
                         end),
                         compute(:result, [:scheduler], fn %{scheduler: s} -> {:ok, "result: #{s}"} end)
                       ]
                     )
                   end
    end

    test "schedule nodes without cycles work fine" do
      graph =
        Journey.new_graph(
          "schedule no cycle test",
          "v1.0.0",
          [
            input(:trigger),
            compute(:data, [:trigger], fn %{trigger: t} -> {:ok, "data: #{t}"} end),
            schedule_once(:scheduler, [:data], fn %{data: d} -> {:ok, "scheduled: #{d}"} end),
            schedule_recurring(:recurring, [:data], fn %{data: d} -> {:ok, "recurring: #{d}"} end)
          ]
        )

      assert graph.name == "schedule no cycle test"
      # 4 defined + 2 auto-added
      assert length(graph.nodes) == 6
    end
  end
end
