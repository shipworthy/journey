defmodule Journey.JourneyListExecutionsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "list_executions" do
    test "sunny day, limit / offset" do
      graph = basic_graph(random_string())
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set_value(:first_name, i)

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 20)
      assert Enum.count(listed_executions) == 20

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 11, offset: 30)
      assert Enum.count(listed_executions) == 11

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 20, offset: 90)
      assert Enum.count(listed_executions) == 10
    end

    test "sunny day, filer by value" do
      graph = basic_graph(random_string())
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set_value(:first_name, i)

      listed_executions = Journey.list_executions(graph_name: graph.name)
      assert Enum.count(listed_executions) == 100

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :lt, 20}])
      assert Enum.count(some_executions) == 19

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :lte, 20}])
      assert Enum.count(some_executions) == 20

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :eq, 50}])
      assert Enum.count(some_executions) == 1

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :neq, 50}])
      assert Enum.count(some_executions) == 99

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :gt, 60}])
      assert Enum.count(some_executions) == 40

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :gte, 60}])
      assert Enum.count(some_executions) == 41

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :in, [20, 22]}])
      assert Enum.count(some_executions) == 2

      neq = fn node_value, val -> node_value != val end
      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, neq, 1}])
      assert Enum.count(some_executions) == 99

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :is_not_nil}])
      assert Enum.count(some_executions) == 100

      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, :is_nil}])
      assert some_executions == []

      is_one = fn node_value -> node_value == 1 end
      some_executions = Journey.list_executions(graph_name: graph.name, value_filters: [{:first_name, is_one}])
      assert Enum.count(some_executions) == 1
    end

    test "sunny day, by graph name" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      Process.sleep(1_000)
      listed_executions = Journey.list_executions(graph_name: execution.graph_name)

      for le <- listed_executions do
        # Making sure that values and computations are loaded.
        assert Enum.count(le.values) == 4
        assert Enum.count(le.computations) == 1, "#{inspect(le.computations)}"
      end

      assert execution.id in (listed_executions |> Enum.map(& &1.id))
    end

    test "sunny day, sort by inserted_at (which is updated after a set_value)" do
      test_id = random_string()

      execution_ids =
        Enum.map(1..3, fn _ ->
          basic_graph(test_id)
          |> Journey.start_execution()
          |> Map.get(:id)
          |> tap(fn _ -> Process.sleep(1_000) end)
        end)

      # Updating the first execution should put it at the back.
      [first_id | remaining_ids] = execution_ids

      updated_execution =
        first_id
        |> Journey.load()
        |> Journey.set_value(:first_name, "Mario")

      expected_order = remaining_ids ++ [first_id]
      {:ok, "Hello, Mario"} = Journey.get_value(updated_execution, :greeting, wait_any: true)

      listed_execution_ids =
        Journey.list_executions(graph_name: basic_graph(test_id).name, order_by_execution_fields: [:updated_at])
        |> Enum.map(fn execution -> execution.id end)
        |> Enum.filter(fn id -> id in execution_ids end)

      assert expected_order == listed_execution_ids
    end

    test "no executions" do
      assert Journey.list_executions(graph_name: "no_such_graph") == []
    end

    test "unexpected option" do
      assert_raise ArgumentError,
                   "Unknown options: [:graph]. Known options: [:graph_name, :graph_version, :include_archived, :limit, :offset, :order_by_execution_fields, :value_filters].",
                   fn ->
                     Journey.list_executions(graph: "no_such_graph")
                   end
    end

    test "filter by graph_version" do
      graph_name = "version_filter_test_#{random_string()}"
      graph_v1 = Journey.new_graph(graph_name, "v1.0.0", [input(:value)])
      graph_v2 = Journey.new_graph(graph_name, "v2.0.0", [input(:value)])

      Journey.start_execution(graph_v1) |> Journey.set_value(:value, "v1_1")
      Journey.start_execution(graph_v1) |> Journey.set_value(:value, "v1_2")
      Journey.start_execution(graph_v2) |> Journey.set_value(:value, "v2_1")

      # Test filtering by version with graph_name
      v1_executions = Journey.list_executions(graph_name: graph_name, graph_version: "v1.0.0")
      assert length(v1_executions) == 2

      v2_executions = Journey.list_executions(graph_name: graph_name, graph_version: "v2.0.0")
      assert length(v2_executions) == 1

      # Test without version filter
      all_executions = Journey.list_executions(graph_name: graph_name)
      assert length(all_executions) == 3
    end

    test "graph_version requires graph_name" do
      assert_raise ArgumentError, "Option :graph_version requires :graph_name to be specified", fn ->
        Journey.list_executions(graph_version: "v1.0.0")
      end
    end

    test "sorting with tuple syntax and mixed formats" do
      test_id = random_string()
      graph = basic_graph(test_id)

      # Create a few executions
      execution_ids =
        Enum.map(1..3, fn i ->
          execution = Journey.start_execution(graph) |> Journey.set_value(:first_name, "User#{i}")
          # Ensure different timestamps (1 second granularity)
          Process.sleep(1100)
          execution.id
        end)

      # Test that tuple syntax doesn't crash and returns results
      desc_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [inserted_at: :desc]
        )

      desc_ids =
        Enum.map(desc_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert length(desc_ids) == 3

      # Test ascending order also works
      asc_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [inserted_at: :asc]
        )

      asc_ids = Enum.map(asc_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)
      assert length(asc_ids) == 3

      # Verify both queries return the same executions
      assert Enum.sort(asc_ids) == Enum.sort(desc_ids)

      # Test mixed directions - multiple sort fields
      mixed_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [graph_name: :asc, inserted_at: :desc]
        )

      mixed_ids =
        Enum.map(mixed_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert length(mixed_ids) == 3

      # Test that the actual sorting direction is correctly applied
      # Create two executions with a clear time difference in a fresh graph
      specific_graph = basic_graph("#{test_id}_specific")
      _e1 = Journey.start_execution(specific_graph) |> Journey.set_value(:first_name, "First")
      Process.sleep(1100)
      _e2 = Journey.start_execution(specific_graph) |> Journey.set_value(:first_name, "Second")

      # Test descending - newer should come first
      desc_specific =
        Journey.list_executions(
          graph_name: specific_graph.name,
          order_by_execution_fields: [inserted_at: :desc]
        )

      # Just verify that both directions work without syntax errors and return expected counts
      asc_specific =
        Journey.list_executions(
          graph_name: specific_graph.name,
          order_by_execution_fields: [inserted_at: :asc]
        )

      assert length(desc_specific) == 2
      assert length(asc_specific) == 2

      # Verify the timestamps are actually different and in the expected order
      desc_timestamps = Enum.map(desc_specific, fn execution -> execution.inserted_at end)
      asc_timestamps = Enum.map(asc_specific, fn execution -> execution.inserted_at end)

      # For descending, larger timestamps should come first
      assert desc_timestamps == Enum.sort(desc_timestamps, :desc)
      # For ascending, smaller timestamps should come first
      assert asc_timestamps == Enum.sort(asc_timestamps, :asc)
    end

    test "atom format functionality" do
      test_id = random_string()
      graph = basic_graph(test_id)

      # Create executions
      execution_ids =
        Enum.map(1..3, fn i ->
          execution = Journey.start_execution(graph) |> Journey.set_value(:first_name, "User#{i}")
          Process.sleep(1100)
          execution.id
        end)

      # Atom format should work (defaults to ascending)
      legacy_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [:inserted_at]
        )

      legacy_ids =
        Enum.map(legacy_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert legacy_ids == execution_ids
    end

    test "mixed format: atoms and tuples together" do
      test_id = random_string()
      graph = basic_graph(test_id)

      # Create executions
      execution_ids =
        Enum.map(1..3, fn i ->
          execution = Journey.start_execution(graph) |> Journey.set_value(:first_name, "User#{i}")
          Process.sleep(1100)
          execution.id
        end)

      # Mixed format: combine atom and tuple syntax
      mixed_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [:graph_name, :revision, inserted_at: :desc]
        )

      mixed_ids =
        Enum.map(mixed_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert length(mixed_ids) == 3
    end

    test "invalid sort field format raises error" do
      graph = basic_graph(random_string())

      # Invalid direction
      assert_raise ArgumentError, ~r/Invalid sort field format.*Expected atom or/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [inserted_at: :invalid]
        )
      end

      # Invalid tuple format
      assert_raise ArgumentError, ~r/Invalid sort field format.*Expected atom or/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [{"invalid", :asc}]
        )
      end

      # Invalid entry type
      assert_raise ArgumentError, ~r/Invalid sort field format.*Expected atom or/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: ["invalid_string"]
        )
      end
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
end
