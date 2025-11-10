defmodule Journey.JourneyCountExecutionsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "count_executions" do
    test "basic counting by graph name" do
      graph = basic_graph(random_string())
      assert Journey.count_executions(graph_name: graph.name) == 0

      Journey.start_execution(graph) |> Journey.set(:first_name, "Alice")
      assert Journey.count_executions(graph_name: graph.name) == 1

      Journey.start_execution(graph) |> Journey.set(:first_name, "Bob")
      assert Journey.count_executions(graph_name: graph.name) == 2

      for i <- 3..10, do: Journey.start_execution(graph) |> Journey.set(:first_name, i)
      assert Journey.count_executions(graph_name: graph.name) == 10
    end

    test "counting with filters" do
      graph = basic_graph(random_string())
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set(:first_name, i)

      assert Journey.count_executions(graph_name: graph.name) == 100
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :lt, 20}]) == 19
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :lte, 20}]) == 20
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, 50}]) == 1
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :neq, 50}]) == 99
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :gt, 60}]) == 40
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :gte, 60}]) == 41
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :in, [20, 22]}]) == 2
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :is_set}]) == 100
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :is_not_set}]) == 0
    end

    test "counting with multiple filters" do
      test_id = random_string()
      graph_name = "multi_filter_count_test_#{test_id}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:age),
            input(:status)
          ]
        )

      Journey.start_execution(graph) |> Journey.set(:age, 25) |> Journey.set(:status, "active")
      Journey.start_execution(graph) |> Journey.set(:age, 17) |> Journey.set(:status, "active")
      Journey.start_execution(graph) |> Journey.set(:age, 30) |> Journey.set(:status, "inactive")
      Journey.start_execution(graph) |> Journey.set(:age, 22) |> Journey.set(:status, "active")

      assert Journey.count_executions(graph_name: graph.name) == 4
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:age, :gte, 18}]) == 3

      assert Journey.count_executions(
               graph_name: graph.name,
               filter_by: [{:age, :gte, 18}, {:status, :eq, "active"}]
             ) == 2
    end

    test "counting with string contains filters" do
      graph = basic_graph(random_string())

      Journey.start_execution(graph) |> Journey.set(:first_name, "alice@gmail.com")
      Journey.start_execution(graph) |> Journey.set(:first_name, "bob@gmail.com")
      Journey.start_execution(graph) |> Journey.set(:first_name, "charlie@yahoo.com")
      Journey.start_execution(graph) |> Journey.set(:first_name, "dave@company.org")

      assert Journey.count_executions(graph_name: graph.name) == 4
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "@gmail"}]) == 2
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, ".com"}]) == 3
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "ALICE"}]) == 1
    end

    test "counting with list_contains filter" do
      graph = list_graph(random_string())

      Journey.start_execution(graph) |> Journey.set(:recipients, ["user1", "user2", "admin"])
      Journey.start_execution(graph) |> Journey.set(:recipients, ["user3", "user4"])
      Journey.start_execution(graph) |> Journey.set(:recipients, [1, 2, 3])
      Journey.start_execution(graph) |> Journey.set(:recipients, ["admin", "user5"])

      assert Journey.count_executions(graph_name: graph.name) == 4

      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "user1"}]) ==
               1

      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "admin"}]) ==
               2

      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, 2}]) == 1
    end

    test "counting by graph version" do
      graph_name = "version_count_test_#{random_string()}"
      graph_v1 = Journey.new_graph(graph_name, "v1.0.0", [input(:value)])
      graph_v2 = Journey.new_graph(graph_name, "v2.0.0", [input(:value)])

      Journey.start_execution(graph_v1) |> Journey.set(:value, "v1_1")
      Journey.start_execution(graph_v1) |> Journey.set(:value, "v1_2")
      Journey.start_execution(graph_v2) |> Journey.set(:value, "v2_1")

      assert Journey.count_executions(graph_name: graph_name) == 3
      assert Journey.count_executions(graph_name: graph_name, graph_version: "v1.0.0") == 2
      assert Journey.count_executions(graph_name: graph_name, graph_version: "v2.0.0") == 1
    end

    test "counting with archived executions" do
      graph = basic_graph(random_string())

      e1 = Journey.start_execution(graph) |> Journey.set(:first_name, "Alice")
      e2 = Journey.start_execution(graph) |> Journey.set(:first_name, "Bob")
      _e3 = Journey.start_execution(graph) |> Journey.set(:first_name, "Charlie")

      assert Journey.count_executions(graph_name: graph.name) == 3

      Journey.archive(e1)
      assert Journey.count_executions(graph_name: graph.name) == 2
      assert Journey.count_executions(graph_name: graph.name, include_archived: true) == 3

      Journey.archive(e2)
      assert Journey.count_executions(graph_name: graph.name) == 1
      assert Journey.count_executions(graph_name: graph.name, include_archived: true) == 3
    end

    test "counting non-existent graph returns zero" do
      assert Journey.count_executions(graph_name: "no_such_graph") == 0
    end

    test "graph_version requires graph_name" do
      assert_raise ArgumentError, "Option :graph_version requires :graph_name to be specified", fn ->
        Journey.count_executions(graph_version: "v1.0.0")
      end
    end

    test "unexpected option raises error" do
      assert_raise ArgumentError,
                   "Unknown options: [:graph]. Known options: [:filter_by, :graph_name, :graph_version, :include_archived, :value_filters].",
                   fn ->
                     Journey.count_executions(graph: "no_such_graph")
                   end
    end

    test "count_executions does not accept sort_by, limit, or offset" do
      graph = basic_graph(random_string())

      # These options should raise errors
      assert_raise ArgumentError, fn ->
        Journey.count_executions(graph_name: graph.name, sort_by: [:inserted_at])
      end

      assert_raise ArgumentError, fn ->
        Journey.count_executions(graph_name: graph.name, limit: 10)
      end

      assert_raise ArgumentError, fn ->
        Journey.count_executions(graph_name: graph.name, offset: 5)
      end
    end

    test "performance: counting vs listing for large result sets" do
      graph = basic_graph(random_string())

      # Create many executions
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set(:first_name, i)

      # Measure count_executions time
      {count_time, count_result} =
        :timer.tc(fn ->
          Journey.count_executions(graph_name: graph.name)
        end)

      # Measure list_executions time
      {list_time, list_result} =
        :timer.tc(fn ->
          Journey.list_executions(graph_name: graph.name) |> Enum.count()
        end)

      # Verify they return the same count
      assert count_result == 100
      assert list_result == 100

      # Count should be faster (allowing some variance for small datasets)
      # This is a performance test to verify optimization works
      assert count_time < list_time,
             "count_executions (#{count_time}μs) should be faster than list_executions (#{list_time}μs)"
    end

    test "counting with mixed data types" do
      graph = basic_graph(random_string())

      # Create executions with string names
      Journey.start_execution(graph) |> Journey.set(:first_name, "alice")
      Journey.start_execution(graph) |> Journey.set(:first_name, "bob")

      # Create executions with integer names
      Journey.start_execution(graph) |> Journey.set(:first_name, 100)
      Journey.start_execution(graph) |> Journey.set(:first_name, 200)

      assert Journey.count_executions(graph_name: graph.name) == 4

      # String filtering should only match string values
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, "alice"}]) == 1

      # Integer filtering should only match numeric values
      assert Journey.count_executions(graph_name: graph.name, filter_by: [{:first_name, :gt, 150}]) == 1
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "basic graph, greetings #{__MODULE__} #{test_id}",
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

  defp list_graph(test_id) do
    Journey.new_graph(
      "list graph, recipients #{__MODULE__} #{test_id}",
      "1.0.0",
      [input(:recipients)]
    )
  end
end
