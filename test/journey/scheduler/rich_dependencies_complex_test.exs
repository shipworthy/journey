defmodule Journey.Scheduler.RichDependenciesComplexTest do
  use ExUnit.Case, async: true

  require Logger

  import Journey.Node
  import Journey.Node.UpstreamDependencies

  defp value_provided?(value_node), do: value_node.set_time != nil

  defp custom_redact(value, atom) when is_atom(atom),
    do: value |> Map.put(atom, 1_234_567_890)

  describe "rich dependencies - complex conditions |" do
    test ":or condition - left", %{test: test_name} do
      graph_name = "graph #{test_name}"

      graph =
        Journey.new_graph(
          graph_name,
          "v2.0.0",
          [
            input(:first_name),
            input(:last_name),
            compute(
              :got_name,
              unblocked_when({
                :or,
                [{:first_name, &value_provided?/1}, {:last_name, &value_provided?/1}]
              }),
              fn _ ->
                {:ok, "name set"}
              end
            )
          ]
        )

      assert graph.name == graph_name

      execution = graph |> Journey.start_execution()
      assert execution != nil

      assert {:error, :not_set} == execution |> Journey.get_value(:got_name, wait_any: 700)
      assert {:error, :not_set} == execution |> Journey.get_value(:first_name)
      assert {:error, :not_set} == execution |> Journey.get_value(:last_name)

      execution = execution |> Journey.set(:first_name, "Mario")
      assert execution != nil
      assert {:ok, "name set"} == execution |> Journey.get_value(:got_name, wait_any: true)
    end

    test ":or condition - right", %{test: test_name} do
      graph_name = "graph #{test_name}"

      graph =
        Journey.new_graph(
          graph_name,
          "v2.0.0",
          [
            input(:first_name),
            input(:last_name),
            compute(
              :got_name,
              unblocked_when({
                :or,
                [{:first_name, &value_provided?/1}, {:last_name, &value_provided?/1}]
              }),
              fn _ ->
                {:ok, "name set"}
              end
            )
          ]
        )

      assert graph.name == graph_name

      execution = graph |> Journey.start_execution()
      assert execution != nil

      assert {:error, :not_set} == execution |> Journey.get_value(:got_name, wait_any: 700)
      assert {:error, :not_set} == execution |> Journey.get_value(:first_name)
      assert {:error, :not_set} == execution |> Journey.get_value(:last_name)

      execution = execution |> Journey.set(:last_name, "Bowser")
      assert execution != nil
      assert {:ok, "name set"} == execution |> Journey.get_value(:got_name, wait_any: true)
    end

    test "nested :and/:or conditions", %{test: test_name} do
      graph_name = "graph #{test_name}"

      graph =
        Journey.new_graph(
          graph_name,
          "v2.0.0",
          [
            input(:g1_a),
            input(:g1_b),
            input(:g2_a),
            input(:g2_b),
            compute(
              :one_of_each_group,
              unblocked_when({
                :and,
                [
                  {
                    :or,
                    [{:g1_a, &value_provided?/1}, {:g1_b, &value_provided?/1}]
                  },
                  {
                    :or,
                    [{:g2_a, &value_provided?/1}, {:g2_b, &value_provided?/1}]
                  }
                ]
              }),
              fn _ ->
                {:ok, "name set"}
              end
            )
          ]
        )

      assert graph.name == graph_name

      execution = graph |> Journey.start_execution()
      assert execution != nil
      assert {:error, :not_set} == execution |> Journey.get_value(:one_of_each_group, wait_any: 700)

      execution = execution |> Journey.set(:g1_a, "g1_a set")
      execution = execution |> Journey.set(:g1_b, "g1_b set")
      assert {:error, :not_set} == execution |> Journey.get_value(:one_of_each_group, wait_any: 700)

      execution = execution |> Journey.set(:g2_a, "g2_a set")
      assert {:ok, "name set"} == execution |> Journey.get_value(:one_of_each_group, wait_any: true)
    end

    test "nested :and/:or conditions with recompute", %{test: test_name} do
      graph_name = "graph #{test_name}"

      graph =
        Journey.new_graph(
          graph_name,
          "v2.0.0",
          [
            input(:g1_a),
            input(:g1_b),
            input(:g2_a),
            input(:g2_b),
            compute(
              :one_of_each_group,
              unblocked_when({
                :and,
                [
                  {
                    :or,
                    [{:g1_a, &value_provided?/1}, {:g1_b, &value_provided?/1}]
                  },
                  {
                    :or,
                    [{:g2_a, &value_provided?/1}, {:g2_b, &value_provided?/1}]
                  }
                ]
              }),
              fn r ->
                r = r |> Map.delete(:one_of_each_group) |> custom_redact(:last_updated_at)
                {:ok, "name set, #{inspect(r, custom_options: [sort_maps: true])}"}
              end
            )
          ]
        )

      assert graph.name == graph_name
      execution = graph |> Journey.start_execution()
      execution = execution |> Journey.set(:g1_a, "g1_a set")
      execution = execution |> Journey.set(:g2_a, "g2_a set")

      assert {:ok,
              "name set, %{execution_id: \"#{execution.id}\", g1_a: \"g1_a set\", g2_a: \"g2_a set\", last_updated_at: 1234567890}"} ==
               execution |> Journey.get_value(:one_of_each_group, wait_any: true)

      execution = execution |> Journey.load()
      assert execution.revision == 4

      execution = execution |> Journey.set(:g1_a, "g1_a set, v2")
      assert execution.revision == 6

      # Fetch and verify the recomputed value when it is available.
      recomputed_value = execution |> Journey.get_value(:one_of_each_group, wait_new: true)

      assert {:ok,
              "name set, %{execution_id: \"#{execution.id}\", g1_a: \"g1_a set, v2\", g2_a: \"g2_a set\", last_updated_at: 1234567890}"} ==
               recomputed_value

      execution = execution |> Journey.load()
      assert execution.revision == 7
    end
  end
end
