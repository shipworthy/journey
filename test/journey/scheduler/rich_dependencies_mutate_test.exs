defmodule Journey.Scheduler.RichDependenciesMutateTest do
  use ExUnit.Case, async: true

  require Logger

  import Journey.Node
  import Journey.Node.UpstreamDependencies

  defp value_provided?(value_node), do: value_node.set_time != nil

  def mario?(value_node) do
    Logger.info("mario? '#{inspect(value_node.node_value)}'")
    value_node.node_value == "Mario"
  end

  describe "rich dependencies - mutations |" do
    test ":and condition with mutate - left to right", %{test: test_name} do
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
                [{:first_name, &mario?/1}, {:last_name, &value_provided?/1}]
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

    test ":and condition with mutate - right to left", %{test: test_name} do
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
                [{:first_name, &mario?/1}, {:last_name, &value_provided?/1}]
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
  end
end
