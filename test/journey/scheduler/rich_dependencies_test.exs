defmodule Journey.Scheduler.RichDependenciesTest do
  use ExUnit.Case, async: true

  @tag :skip

  require Logger

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  # def provided?(value_node), do: value_node.set_time != nil
  def taurus?(zodiac_sign_value_node), do: zodiac_sign_value_node.node_value == "Taurus"
  def aries?(zodiac_sign_value_node), do: zodiac_sign_value_node.node_value == "Aries"

  def mario?(value_node) do
    Logger.info("mario? '#{inspect(value_node.node_value)}'")
    value_node.node_value == "Mario"
  end

  def scorpio?(zodiac_sign_value_node), do: zodiac_sign_value_node.node_value == "Scorpio"

  def set_10_minutes_ago?(value_node) do
    value_node.set_time != nil and value_node.set_time <= System.system_time(:second) - 600
  end

  describe "rich dependencies |" do
    #    test "horoscopes for taurus and others" do
    #      graph =
    #        Journey.new_graph(
    #          "graph 1",
    #          "v2.0.0",
    #          [
    #            input(:first_name),
    #            input(:birth_day),
    #            input(:birth_month),
    #            compute(
    #              :zodiac_sign,
    #              [:birth_month, :birth_day],
    #              fn %{birth_month: birth_month, birth_day: birth_day} ->
    #                if (birth_month == "April" and birth_day >= 20) or (birth_month == "May" and birth_day <= 20) do
    #                  {:ok, "Taurus"}
    #                else
    #                  {:ok, "not a Taurus"}
    #                end
    #              end
    #            ),
    #            compute(
    #              :horoscope_for_taurus,
    #              unblocked_when(
    #                or_(
    #                  and_(
    #                    or_(
    #                      satisfies(:zodiac_sign, &taurus?/1),
    #                      satisfies(:zodiac_sign, &aries?/1)
    #                    ),
    #                    satisfies(:first_name, &provided?/1)
    #                  ),
    #                  satisfies(:zodiac_sign, &scorpio?/1)
    #                )
    #              ),
    #              #              [and_(unblocked_when(:zodiac_sign, &taurus?/1), unblocked_when(:first_name, &provided?/1)],
    #              fn %{zodiac_sign: zodiac_sign, first_name: first_name} = _values ->
    #                {:ok, "Good things await, #{zodiac_sign} #{first_name}!"}
    #              end
    #            ),
    #            compute(
    #              :horoscope_for_not_taurus,
    #              [zodiac_sign: fn value_node -> !taurus?(value_node) end, first_name: &provided?/1],
    #              fn %{zodiac_sign: _zodiac_sign, first_name: first_name} = _values ->
    #                {:ok, "Ah, too bad you are not a Taurus, #{first_name}. Good luck, you are going to need it!"}
    #              end
    #            )
    #          ]
    #        )
    #
    #      assert graph.name == "graph 1"
    #    end
    #

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
              unblocked_when({
                :lol,
                [
                  {
                    :or,
                    [{:g1_a, &provided?/1}, {:g1_b, &provided?/1}]
                  },
                  {
                    :or,
                    [{:g2_a, &provided?/1}, {:g2_b, &provided?/1}]
                  }
                ]
              }),
              fn _ ->
                {:ok, "name set"}
              end
            )
          ]
        )
      end
    end

    test "recursive, recompute", %{test: test_name} do
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
                    [{:g1_a, &provided?/1}, {:g1_b, &provided?/1}]
                  },
                  {
                    :or,
                    [{:g2_a, &provided?/1}, {:g2_b, &provided?/1}]
                  }
                ]
              }),
              fn r ->
                r = r |> Map.delete(:one_of_each_group) |> redact(:last_updated_at)
                {:ok, "name set, #{inspect(r, custom_options: [sort_maps: true])}"}
              end
            )
          ]
        )

      assert graph.name == graph_name
      execution = graph |> Journey.start_execution()
      execution = execution |> Journey.set_value(:g1_a, "g1_a set")
      execution = execution |> Journey.set_value(:g2_a, "g2_a set")

      assert {:ok,
              "name set, %{execution_id: \"#{execution.id}\", g1_a: \"g1_a set\", g2_a: \"g2_a set\", last_updated_at: 1234567890}"} ==
               execution |> Journey.get_value(:one_of_each_group, wait_any: true)

      execution = execution |> Journey.load()
      assert execution.revision == 4

      execution = execution |> Journey.set_value(:g1_a, "g1_a set, v2")
      execution = execution |> Journey.load()
      assert execution.revision == 6

      # Fetch and verify the recomputed value when it is available.
      recomputed_value = execution |> Journey.get_value(:one_of_each_group, wait_new: true)

      assert {:ok,
              "name set, %{execution_id: \"#{execution.id}\", g1_a: \"g1_a set, v2\", g2_a: \"g2_a set\", last_updated_at: 1234567890}"} ==
               recomputed_value

      execution = execution |> Journey.load()
      assert execution.revision == 7
    end

    test "recursive", %{test: test_name} do
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
                    [{:g1_a, &provided?/1}, {:g1_b, &provided?/1}]
                  },
                  {
                    :or,
                    [{:g2_a, &provided?/1}, {:g2_b, &provided?/1}]
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

      execution = execution |> Journey.set_value(:g1_a, "g1_a set")
      execution = execution |> Journey.set_value(:g1_b, "g1_b set")
      assert {:error, :not_set} == execution |> Journey.get_value(:one_of_each_group, wait_any: 700)

      execution = execution |> Journey.set_value(:g2_a, "g2_a set")
      assert {:ok, "name set"} == execution |> Journey.get_value(:one_of_each_group, wait_any: true)
    end

    test "two conditions, :or, left", %{test: test_name} do
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
                [{:first_name, &provided?/1}, {:last_name, &provided?/1}]
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

      execution = execution |> Journey.set_value(:first_name, "Mario")
      assert execution != nil
      assert {:ok, "name set"} == execution |> Journey.get_value(:got_name, wait_any: true)
    end

    test "two conditions, :or, right", %{test: test_name} do
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
                [{:first_name, &provided?/1}, {:last_name, &provided?/1}]
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

      execution = execution |> Journey.set_value(:last_name, "Bowser")
      assert execution != nil
      assert {:ok, "name set"} == execution |> Journey.get_value(:got_name, wait_any: true)
    end

    test "two conditions, :and, left to right", %{test: test_name} do
      graph_name = "graph #{test_name}"

      graph =
        Journey.new_graph(
          graph_name,
          "v2.0.0",
          [
            input(:first_name),
            input(:last_name),
            mutate(
              :remove_pii_for_mario,
              unblocked_when({
                :and,
                [{:first_name, &mario?/1}, {:last_name, &provided?/1}]
              }),
              fn _ ->
                {:ok, "redacted"}
              end,
              mutates: :first_name
            )
          ]
        )

      assert graph.name == graph_name

      execution = graph |> Journey.start_execution()
      assert execution != nil

      assert {:error, :not_set} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: 700)
      assert {:error, :not_set} == execution |> Journey.get_value(:first_name, wait_any: 700)

      execution = execution |> Journey.set_value(:first_name, "Mario")
      assert {:error, :not_set} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: 700)

      execution = execution |> Journey.set_value(:last_name, "Bowser")
      assert execution != nil
      assert {:ok, "updated :first_name"} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: true)
      assert {:ok, "redacted"} == execution |> Journey.get_value(:first_name, wait_any: true)
    end

    test "two conditions, :and, right to left", %{test: test_name} do
      graph_name = "graph #{test_name}"

      graph =
        Journey.new_graph(
          graph_name,
          "v2.0.0",
          [
            input(:first_name),
            input(:last_name),
            mutate(
              :remove_pii_for_mario,
              unblocked_when({
                :and,
                [{:first_name, &mario?/1}, {:last_name, &provided?/1}]
              }),
              fn _ ->
                {:ok, "redacted"}
              end,
              mutates: :first_name
            )
          ]
        )

      assert graph.name == graph_name

      execution = graph |> Journey.start_execution()
      assert execution != nil

      assert {:error, :not_set} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: 700)
      assert {:error, :not_set} == execution |> Journey.get_value(:first_name, wait_any: 700)

      execution = execution |> Journey.set_value(:last_name, "Bowser")
      assert {:error, :not_set} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: 700)

      execution = execution |> Journey.set_value(:first_name, "Mario")
      assert execution != nil
      assert {:ok, "updated :first_name"} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: true)
      assert {:ok, "redacted"} == execution |> Journey.get_value(:first_name, wait_any: true)
    end

    test "single condition (value?)" do
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

      execution = execution |> Journey.set_value(:first_name, "Mario")
      assert execution != nil
      assert {:ok, "updated :first_name"} == execution |> Journey.get_value(:remove_pii_for_mario, wait_any: true)
      assert {:ok, "redacted"} == execution |> Journey.get_value(:first_name, wait_any: true)
    end

    #    test "scheduled cleanup as functions" do
    #      graph =
    #        Journey.new_graph(
    #          "graph 2",
    #          "v2.0.0",
    #          [
    #            input(:first_name),
    #            mutate(
    #              :remove_pii,
    #              unblocked_when(satisfies(:first_name, &mario?/1)),
    #              # unblocked_when(satisfies(:first_name, &set_10_minutes_ago?/1)),
    #              # [first_name: &set_10_minutes_ago?/1],
    #              fn _ ->
    #                {:ok, "redacted"}
    #              end,
    #              mutates: :first_name
    #            )
    #          ]
    #        )
    #
    #      assert graph.name == "graph 2"
    #
    #      # execution =
    #      graph
    #      |> Journey.start_execution()
    #      |> IO.inspect(label: :start_execution)
    #      |> Journey.set_value(:first_name, "Mario")
    #      |> IO.inspect(label: :set_value)
    #      |> Journey.get_value(:remove_pii, wait_any: true)
    #      |> IO.inspect(label: :value)
    #    end
  end
end
