defmodule Journey.Graph.ValidationsTest do
  use ExUnit.Case, async: true

  import Journey.Node

  describe "mutate with update_revision_on_change cycle detection |" do
    test "mutate with update_revision_on_change: true mutating an upstream node raises error" do
      assert_raise RuntimeError,
                   ~r/Mutation node ':normalize_value' with update_revision_on_change: true creates a cycle by mutating ':value' which is in its upstream dependencies/,
                   fn ->
                     Journey.new_graph(
                       "invalid cycle graph",
                       "v1.0.0",
                       [
                         input(:value),
                         mutate(
                           :normalize_value,
                           [:value],
                           fn %{value: v} -> {:ok, String.downcase(v)} end,
                           mutates: :value,
                           update_revision_on_change: true
                         )
                       ]
                     )
                   end
    end

    test "mutate with update_revision_on_change: false mutating an upstream node is allowed" do
      # This should not raise - mutations without update_revision_on_change can mutate upstream nodes
      graph =
        Journey.new_graph(
          "valid mutation of upstream",
          "v1.0.0",
          [
            input(:value),
            mutate(
              :change_value,
              [:value],
              fn %{value: _v} -> {:ok, "changed"} end,
              mutates: :value,
              update_revision_on_change: false
            )
          ]
        )

      assert graph != nil
    end

    test "mutate with update_revision_on_change: true mutating a non-upstream node is allowed" do
      # This should not raise - the mutated node is not in upstream dependencies
      graph =
        Journey.new_graph(
          "valid mutation of non-upstream",
          "v1.0.0",
          [
            input(:trigger),
            input(:target),
            mutate(
              :update_target,
              [:trigger],
              fn _ -> {:ok, "updated"} end,
              mutates: :target,
              update_revision_on_change: true
            )
          ]
        )

      assert graph != nil
    end

    test "mutate with update_revision_on_change: true in a chain (schedule + mutate + compute) is valid" do
      # The rideshare pattern should be valid
      graph =
        Journey.new_graph(
          "rideshare location pattern",
          "v1.0.0",
          [
            schedule_recurring(
              :location_schedule,
              [],
              fn _ -> {:ok, System.system_time(:second) + 5} end
            ),
            input(:driver_location),
            mutate(
              :update_location,
              [:location_schedule],
              fn %{driver_location: loc} -> {:ok, (loc || 0) + 1} end,
              mutates: :driver_location,
              update_revision_on_change: true
            ),
            compute(
              :eta,
              [:driver_location],
              fn %{driver_location: loc} -> {:ok, "ETA: #{100 - loc} min"} end
            )
          ]
        )

      assert graph != nil
    end

    test "complex upstream dependency chain with cycle detection" do
      assert_raise RuntimeError,
                   ~r/Mutation node ':mutate_b' with update_revision_on_change: true creates a cycle by mutating ':value_b' which is in its upstream dependencies/,
                   fn ->
                     Journey.new_graph(
                       "complex cycle",
                       "v1.0.0",
                       [
                         input(:value_a),
                         compute(
                           :value_b,
                           [:value_a],
                           fn %{value_a: a} -> {:ok, a * 2} end
                         ),
                         mutate(
                           :mutate_b,
                           [:value_b],
                           fn %{value_b: b} -> {:ok, b + 1} end,
                           mutates: :value_b,
                           update_revision_on_change: true
                         )
                       ]
                     )
                   end
    end
  end
end
