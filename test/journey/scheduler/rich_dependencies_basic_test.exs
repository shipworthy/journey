defmodule Journey.Scheduler.RichDependenciesBasicTest do
  use ExUnit.Case, async: true

  require Logger

  import Journey.Node
  import Journey.Node.UpstreamDependencies

  defp value_provided?(value_node), do: value_node.set_time != nil

  def mario?(value_node) do
    Logger.info("mario? '#{inspect(value_node.node_value)}'")
    value_node.node_value == "Mario"
  end

  describe "rich dependencies - basic conditions |" do
    test "single condition (:provided? condition)" do
      graph =
        Journey.new_graph(
          "graph 2",
          "v2.0.0",
          [
            input(:first_name),
            mutate(
              :remove_pii_for_mario,
              unblocked_when({:first_name, &mario?/1}),
              fn _ ->
                {:ok, "redacted"}
              end,
              mutates: :first_name
            )
          ]
        )

      assert graph.name == "graph 2"

      execution = graph |> Journey.start_execution()
      assert execution != nil

      assert {:error, :not_set} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: 700)
      assert {:error, :not_set} == execution |> Journey.get_value(:first_name, wait_any: 700)

      execution = execution |> Journey.set(:first_name, "Mario")

      assert {:ok, "updated :first_name"} =
               execution |> Journey.get_value(:remove_pii_for_mario, wait_any: true)

      assert {:ok, "redacted"} = execution |> Journey.get_value(:first_name, wait_any: true)
    end

    test "bad unblocked_when", %{test: test_name} do
      assert_raise ArgumentError, ~r/Invalid unblocked_when expression/, fn ->
        Journey.new_graph(
          "graph #{test_name}",
          "v2.0.0",
          [
            input(:g1_a),
            input(:g1_b),
            input(:g2_a),
            input(:g2_b),
            compute(
              :one_of_each_group,
              # apply/3 keeps this intentionally-invalid unblocked_when expression opaque to
              # the compile-time type checker (otherwise it infers none() and warns that the
              # value flows into compute/3), while preserving the runtime ArgumentError this
              # test asserts. See also the standalone "bad unblocked_when (direct call)" test.
              # credo:disable-for-next-line Credo.Check.Refactor.Apply
              apply(Journey.Node.UpstreamDependencies, :unblocked_when, [
                {
                  :lol,
                  [
                    {:or, [{:g1_a, &value_provided?/1}, {:g1_b, &value_provided?/1}]},
                    {:or, [{:g2_a, &value_provided?/1}, {:g2_b, &value_provided?/1}]}
                  ]
                }
              ]),
              fn _ ->
                {:ok, "name set"}
              end
            )
          ]
        )
      end
    end

    test "bad unblocked_when (direct call)" do
      # The graph-scaffolded test above never actually runs compute/3 or new_graph/3:
      # unblocked_when/1 raises during argument evaluation, before those run. This asserts
      # the same validation behavior directly against unblocked_when/1, where it actually
      # happens — and reads clearly as a unit test of that function.
      assert_raise ArgumentError, ~r/Invalid unblocked_when expression/, fn ->
        unblocked_when({
          :lol,
          [
            {:or, [{:g1_a, &value_provided?/1}, {:g1_b, &value_provided?/1}]},
            {:or, [{:g2_a, &value_provided?/1}, {:g2_b, &value_provided?/1}]}
          ]
        })
      end
    end
  end
end
