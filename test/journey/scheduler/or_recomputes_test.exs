defmodule Journey.Scheduler.OrRecomputeTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "recompute of or conditions" do
    test "sunny day" do
      g =
        Journey.new_graph("test or recomputes #{__MODULE__}", random_string(), [
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

      e =
        g
        |> Journey.start_execution()
        |> Journey.set(:a, "a")

      {:ok, %{value: ab, revision: rev}} = Journey.get(e, :a_or_b, wait: :any)
      assert ab == "a_"

      Journey.set(e, :b, "b")
      {:ok, %{value: ab, revision: _rev}} = Journey.get(e, :a_or_b, wait: {:newer_than, rev})

      assert ab == "ab"
    end
  end
end
