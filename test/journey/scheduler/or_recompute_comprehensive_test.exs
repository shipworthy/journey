defmodule Journey.Scheduler.OrRecomputeComprehensiveTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "OR condition recomputation" do
    test "basic OR - setting second input triggers recomputation" do
      g =
        Journey.new_graph("or basic #{random_string()}", random_string(), [
          input(:a),
          input(:b),
          compute(
            :a_or_b,
            unblocked_when({
              :or,
              [
                {:a, &provided?/1},
                {:b, &provided?/1}
              ]
            }),
            fn x ->
              {:ok, Map.get(x, :a, "_") <> Map.get(x, :b, "_")}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set first input - should compute with placeholder for b
      e = Journey.set(e, :a, "a")
      {:ok, %{value: result}} = Journey.get(e, :a_or_b, wait: :any)
      assert result == "a_"

      # Set second input - should recompute with both values
      e = Journey.set(e, :b, "b")
      {:ok, %{value: result}} = Journey.get(e, :a_or_b, wait: :newer)
      assert result == "ab"
    end

    test "three-way OR - sequential sets trigger recomputation each time" do
      g =
        Journey.new_graph("or three-way #{random_string()}", random_string(), [
          input(:a),
          input(:b),
          input(:c),
          compute(
            :a_or_b_or_c,
            unblocked_when({
              :or,
              [
                {:a, &provided?/1},
                {:b, &provided?/1},
                {:c, &provided?/1}
              ]
            }),
            fn x ->
              {:ok, Map.get(x, :a, "_") <> Map.get(x, :b, "_") <> Map.get(x, :c, "_")}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set first input
      e = Journey.set(e, :a, "a")
      {:ok, %{value: result}} = Journey.get(e, :a_or_b_or_c, wait: :any)
      assert result == "a__"

      # Set second input - should recompute
      e = Journey.set(e, :b, "b")
      {:ok, %{value: result}} = Journey.get(e, :a_or_b_or_c, wait: :newer)
      assert result == "ab_"

      # Set third input - should recompute again
      e = Journey.set(e, :c, "c")
      {:ok, %{value: result}} = Journey.get(e, :a_or_b_or_c, wait: :newer)
      assert result == "abc"
    end

    test "OR is order-independent - same final result regardless of set order" do
      g =
        Journey.new_graph("or order independent #{random_string()}", random_string(), [
          input(:x),
          input(:y),
          compute(
            :x_or_y,
            unblocked_when({
              :or,
              [
                {:x, &provided?/1},
                {:y, &provided?/1}
              ]
            }),
            fn vals ->
              {:ok, Map.get(vals, :x, "_") <> Map.get(vals, :y, "_")}
            end
          )
        ])

      # Execution 1: set x then y
      e1 = Journey.start_execution(g)
      e1 = Journey.set(e1, :x, "X")
      {:ok, _} = Journey.get(e1, :x_or_y, wait: :any)
      e1 = Journey.set(e1, :y, "Y")
      {:ok, %{value: result1}} = Journey.get(e1, :x_or_y, wait: :newer)

      # Execution 2: set y then x
      e2 = Journey.start_execution(g)
      e2 = Journey.set(e2, :y, "Y")
      {:ok, _} = Journey.get(e2, :x_or_y, wait: :any)
      e2 = Journey.set(e2, :x, "X")
      {:ok, %{value: result2}} = Journey.get(e2, :x_or_y, wait: :newer)

      # Both should have the same final result
      assert result1 == "XY"
      assert result2 == "XY"
    end
  end

  describe "AND condition behavior (control test)" do
    test "AND conditions still work - no spurious recomputation" do
      g =
        Journey.new_graph("and control #{random_string()}", random_string(), [
          input(:p),
          input(:q),
          compute(
            :p_and_q,
            [:p, :q],
            fn %{p: p, q: q} ->
              {:ok, "#{p}-#{q}"}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set first input - should NOT compute yet (waiting for q)
      e = Journey.set(e, :p, "P")
      assert {:error, :not_set} = Journey.get(e, :p_and_q)

      # Set second input - should compute now
      e = Journey.set(e, :q, "Q")
      {:ok, %{value: result}} = Journey.get(e, :p_and_q, wait: :any)
      assert result == "P-Q"

      # Update first input - should recompute
      e = Journey.set(e, :p, "P2")
      {:ok, %{value: result}} = Journey.get(e, :p_and_q, wait: :newer)
      assert result == "P2-Q"
    end
  end

  describe "nested conditions" do
    test "OR inside AND - recomputes when OR branch changes" do
      g =
        Journey.new_graph("nested or-and #{random_string()}", random_string(), [
          input(:a),
          input(:b),
          input(:c),
          compute(
            :nested,
            unblocked_when({
              :and,
              [
                {
                  :or,
                  [
                    {:a, &provided?/1},
                    {:b, &provided?/1}
                  ]
                },
                {:c, &provided?/1}
              ]
            }),
            fn vals ->
              {:ok, "#{Map.get(vals, :a, "_")}-#{Map.get(vals, :b, "_")}-#{Map.get(vals, :c, "_")}"}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set a (satisfies first branch of OR)
      e = Journey.set(e, :a, "A")

      # Set c (satisfies the AND)
      e = Journey.set(e, :c, "C")
      {:ok, %{value: result}} = Journey.get(e, :nested, wait: :any)
      assert result == "A-_-C"

      # Set b (should recompute - OR has new satisfied branch)
      e = Journey.set(e, :b, "B")
      {:ok, %{value: result}} = Journey.get(e, :nested, wait: :newer)
      assert result == "A-B-C"
    end

    test "AND inside OR - complex nested conditions" do
      g =
        Journey.new_graph("nested and-or #{random_string()}", random_string(), [
          input(:x),
          input(:y),
          input(:z),
          compute(
            :nested,
            unblocked_when({
              :or,
              [
                {
                  :and,
                  [
                    {:x, &provided?/1},
                    {:y, &provided?/1}
                  ]
                },
                {:z, &provided?/1}
              ]
            }),
            fn vals ->
              {:ok, "#{Map.get(vals, :x, "_")}#{Map.get(vals, :y, "_")}#{Map.get(vals, :z, "_")}"}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set z - satisfies OR immediately
      e = Journey.set(e, :z, "Z")
      {:ok, %{value: result}} = Journey.get(e, :nested, wait: :any)
      assert result == "__Z"

      # Set x - should recompute
      e = Journey.set(e, :x, "X")
      {:ok, %{value: result}} = Journey.get(e, :nested, wait: :newer)
      assert result == "X_Z"

      # Set y - should recompute again (AND branch now fully satisfied)
      e = Journey.set(e, :y, "Y")
      {:ok, %{value: result}} = Journey.get(e, :nested, wait: :newer)
      assert result == "XYZ"
    end
  end

  describe "NOT conditions" do
    test "NOT condition triggers recomputation when dependency changes" do
      g =
        Journey.new_graph("not recompute #{random_string()}", random_string(), [
          input(:flag),
          compute(
            :not_flag,
            unblocked_when({
              :not,
              {:flag, &provided?/1}
            }),
            fn _vals ->
              {:ok, "flag_not_set"}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Initially, flag is not set - should compute
      {:ok, %{value: result}} = Journey.get(e, :not_flag, wait: :any)
      assert result == "flag_not_set"

      # Set flag - should become unblocked (NOT is no longer satisfied)
      # The node should be invalidated
      e = Journey.set(e, :flag, true)

      # After setting flag, the NOT condition is no longer met
      # So the node should be unset
      assert {:error, :not_set} = Journey.get(e, :not_flag)
    end

    test "NOT with true?/false? conditions" do
      g =
        Journey.new_graph("not with boolean #{random_string()}", random_string(), [
          input(:enabled),
          compute(
            :when_disabled,
            unblocked_when({
              :not,
              {:enabled, &true?/1}
            }),
            fn _vals ->
              {:ok, "disabled"}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set enabled to false - NOT should be satisfied
      e = Journey.set(e, :enabled, false)
      {:ok, %{value: result}} = Journey.get(e, :when_disabled, wait: :any)
      assert result == "disabled"

      # Change enabled to true - NOT is no longer satisfied
      e = Journey.set(e, :enabled, true)

      # The node should be unset now (NOT condition no longer met)
      assert {:error, :not_set} = Journey.get(e, :when_disabled)
    end
  end

  describe "edge cases" do
    test "all OR branches set simultaneously" do
      g =
        Journey.new_graph("or simultaneous #{random_string()}", random_string(), [
          input(:a),
          input(:b),
          compute(
            :result,
            unblocked_when({
              :or,
              [
                {:a, &provided?/1},
                {:b, &provided?/1}
              ]
            }),
            fn vals ->
              {:ok, "#{Map.get(vals, :a, "?")}+#{Map.get(vals, :b, "?")}"}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set both at once using map form
      e = Journey.set(e, %{a: "A", b: "B"})
      {:ok, %{value: result}} = Journey.get(e, :result, wait: :any)
      assert result == "A+B"
    end

    test "updating already-satisfied OR branch triggers recomputation" do
      g =
        Journey.new_graph("or update same branch #{random_string()}", random_string(), [
          input(:m),
          input(:n),
          compute(
            :result,
            unblocked_when({
              :or,
              [
                {:m, &provided?/1},
                {:n, &provided?/1}
              ]
            }),
            fn vals ->
              {:ok, "m=#{Map.get(vals, :m, "?")},n=#{Map.get(vals, :n, "?")}"}
            end
          )
        ])

      e = Journey.start_execution(g)

      # Set m
      e = Journey.set(e, :m, "1")
      {:ok, %{value: result}} = Journey.get(e, :result, wait: :any)
      assert result == "m=1,n=?"

      # Update m again (same branch, new value)
      e = Journey.set(e, :m, "2")
      {:ok, %{value: result}} = Journey.get(e, :result, wait: :newer)
      assert result == "m=2,n=?"

      # Now set n (different branch)
      e = Journey.set(e, :n, "3")
      {:ok, %{value: result}} = Journey.get(e, :result, wait: :newer)
      assert result == "m=2,n=3"
    end
  end
end
