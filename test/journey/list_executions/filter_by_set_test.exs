defmodule Journey.JourneyListExecutionsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "filter_by" do
    setup do
      test_id = random_string()
      {:ok, %{test_id: test_id, graph: basic_graph(test_id)}}
    end

    test "is_set", %{graph: g} do
      execution =
        g
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      _ = Journey.get(execution, :greeting, wait: :any)

      [the_one_execution] =
        Journey.list_executions(graph_name: g.name, graph_version: g.version, filter_by: [{:first_name, :is_set}])

      assert execution.id == the_one_execution.id

      [the_one_execution] =
        Journey.list_executions(graph_name: g.name, graph_version: g.version, filter_by: [{:greeting, :is_set}])

      assert execution.id == the_one_execution.id

      assert [] ==
               Journey.list_executions(
                 graph_name: g.name,
                 graph_version: g.version,
                 filter_by: [{:first_name, :is_not_set}]
               )

      assert [] ==
               Journey.list_executions(
                 graph_name: g.name,
                 graph_version: g.version,
                 filter_by: [{:greeting, :is_not_set}]
               )
    end

    test "is_not_set", %{graph: g} do
      execution = g |> Journey.start_execution()

      [the_one_execution] =
        Journey.list_executions(
          graph_name: g.name,
          graph_version: g.version,
          filter_by: [{:first_name, :is_not_set}]
        )

      assert execution.id == the_one_execution.id

      [the_one_execution] =
        Journey.list_executions(
          graph_name: g.name,
          graph_version: g.version,
          filter_by: [{:greeting, :is_not_set}]
        )

      assert execution.id == the_one_execution.id

      assert [] ==
               Journey.list_executions(
                 graph_name: g.name,
                 graph_version: g.version,
                 filter_by: [{:first_name, :is_set}]
               )

      assert [] ==
               Journey.list_executions(
                 graph_name: g.name,
                 graph_version: g.version,
                 filter_by: [{:greeting, :is_set}]
               )
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "basic graph #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:first_name),
        compute(
          :greeting,
          unblocked_when({:first_name, &provided?/1}),
          fn %{first_name: first_name} ->
            {:ok, "Hello, #{first_name}"}
          end
        )
      ]
    )
  end
end
