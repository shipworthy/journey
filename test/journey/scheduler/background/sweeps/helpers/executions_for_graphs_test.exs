defmodule Journey.Scheduler.Background.Sweeps.Helpers.ExecutionsForGraphsTest do
  use ExUnit.Case, async: true
  import Journey.Node
  import Journey.Helpers.Random
  alias Journey.Scheduler.Background.Sweeps.Helpers

  describe "executions_for_graphs/2" do
    test "filters executions by selected graphs only" do
      # Define three different graphs with unique names
      graph_a =
        Journey.new_graph(
          "exec_graph_a_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      graph_b =
        Journey.new_graph(
          "exec_graph_b_#{random_string()}",
          "1.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, a + 10} end)
          ]
        )

      graph_c =
        Journey.new_graph(
          "exec_graph_c_#{random_string()}",
          "1.0",
          [
            input(:p),
            compute(:q, [:p], fn %{p: p} -> {:ok, p * p} end)
          ]
        )

      # Start executions for each graph
      exec_a = Journey.start_execution(graph_a)
      exec_b = Journey.start_execution(graph_b)
      exec_c = Journey.start_execution(graph_c)

      # Build graph tuples explicitly for only graph_a and graph_c (excluding graph_b)
      graph_tuples = [{graph_a.name, "1.0"}, {graph_c.name, "1.0"}]

      # Query executions for selected graphs only
      query = Helpers.executions_for_graphs(nil, graph_tuples)
      executions = Journey.Repo.all(query)

      # Verify we only get executions from graph_a and graph_c
      execution_ids = Enum.map(executions, & &1.id)

      assert exec_a.id in execution_ids
      assert exec_c.id in execution_ids
      refute exec_b.id in execution_ids
    end

    test "returns empty list when no graphs match" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "exec_test_graph_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution
      _execution = Journey.start_execution(graph)

      # Query with non-existent graph
      query = Helpers.executions_for_graphs(nil, [{"non_existent", "1.0"}])
      executions = Journey.Repo.all(query)

      assert executions == []
    end

    test "handles multiple versions of same graph" do
      # Create two versions of the same graph with unique name
      graph_name = "exec_versioned_graph_#{random_string()}"

      graph_v1 =
        Journey.new_graph(
          graph_name,
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      graph_v2 =
        Journey.new_graph(
          graph_name,
          "2.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 3} end)
          ]
        )

      # Start executions for both versions
      exec_v1 = Journey.start_execution(graph_v1)
      exec_v2 = Journey.start_execution(graph_v2)

      # Build graph tuples explicitly - only include v2.0
      graph_tuples = [{graph_name, "2.0"}]

      # Query should only return executions from v2.0
      query = Helpers.executions_for_graphs(nil, graph_tuples)
      executions = Journey.Repo.all(query)

      execution_ids = Enum.map(executions, & &1.id)

      refute exec_v1.id in execution_ids
      assert exec_v2.id in execution_ids
    end

    test "returns empty list when called with empty graph list" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "exec_empty_test_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution
      _execution = Journey.start_execution(graph)

      # Query with empty graph list
      query = Helpers.executions_for_graphs(nil, [])
      executions = Journey.Repo.all(query)

      # Should return no executions even though executions exist
      assert executions == []
    end

    test "excludes archived executions from results" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "exec_archived_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start two executions
      exec_1 = Journey.start_execution(graph)
      exec_2 = Journey.start_execution(graph)

      # Archive one execution
      Journey.Executions.archive_execution(exec_1.id)

      # Build graph tuples explicitly from our test graph
      graph_tuples = [{graph.name, "1.0"}]

      # Query executions - should only include non-archived
      query = Helpers.executions_for_graphs(nil, graph_tuples)
      executions = Journey.Repo.all(query)

      execution_ids = Enum.map(executions, & &1.id)

      # Only non-archived execution should be in the results
      refute exec_1.id in execution_ids
      assert exec_2.id in execution_ids
    end
  end

  describe "executions_for_graphs/2 with execution_id" do
    test "filters specific execution when graph is in the list" do
      # Create multiple graphs with unique names
      graph_a =
        Journey.new_graph(
          "exec_specific_a_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      graph_b =
        Journey.new_graph(
          "exec_specific_b_#{random_string()}",
          "1.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, a + 10} end)
          ]
        )

      # Start executions for each graph
      exec_a = Journey.start_execution(graph_a)
      _exec_b = Journey.start_execution(graph_b)

      # Build graph tuples explicitly from our test graphs
      graph_tuples = [{graph_a.name, "1.0"}, {graph_b.name, "1.0"}]

      # Query with specific execution_id
      query = Helpers.executions_for_graphs(exec_a.id, graph_tuples)
      executions = Journey.Repo.all(query)

      # Should only return exec_a
      assert length(executions) == 1
      assert List.first(executions).id == exec_a.id
    end

    test "returns empty list when execution's graph is not in the graph list" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "exec_unregistered_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution
      execution = Journey.start_execution(graph)

      # Build graph tuples that don't include this graph
      graph_tuples = [{"some_other_graph_#{random_string()}", "1.0"}]

      # Query with execution_id but graph is not in the list
      query = Helpers.executions_for_graphs(execution.id, graph_tuples)
      executions = Journey.Repo.all(query)

      # Should return empty even though execution exists
      assert executions == []
    end

    test "returns empty list when called with empty graph list" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "exec_empty_specific_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution
      execution = Journey.start_execution(graph)

      # Query with execution_id and empty graph list
      query = Helpers.executions_for_graphs(execution.id, [])
      executions = Journey.Repo.all(query)

      # Should return empty
      assert executions == []
    end

    test "filters correctly when multiple executions exist for the same graph" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "exec_multiple_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start two executions from the same graph
      exec_1 = Journey.start_execution(graph)
      _exec_2 = Journey.start_execution(graph)

      # Build graph tuples explicitly from our test graph
      graph_tuples = [{graph.name, "1.0"}]

      # Query with exec_1's id
      query = Helpers.executions_for_graphs(exec_1.id, graph_tuples)
      executions = Journey.Repo.all(query)

      # Should only return exec_1
      assert length(executions) == 1
      assert List.first(executions).id == exec_1.id
    end

    test "returns empty list for non-existent execution_id" do
      # Create a graph to ensure there are executions in the system
      graph =
        Journey.new_graph(
          "exec_nonexistent_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution to ensure there are executions in the system
      _execution = Journey.start_execution(graph)

      # Build graph tuples explicitly from our test graph
      graph_tuples = [{graph.name, "1.0"}]

      # Query with non-existent execution_id
      fake_execution_id = "EXEC_FAKE_#{random_string()}"
      query = Helpers.executions_for_graphs(fake_execution_id, graph_tuples)
      executions = Journey.Repo.all(query)

      # Should return empty
      assert executions == []
    end

    test "correctly handles execution_id that exists but doesn't match any listed graphs" do
      # Create two graphs with unique names
      graph_included =
        Journey.new_graph(
          "exec_included_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      graph_excluded =
        Journey.new_graph(
          "exec_excluded_#{random_string()}",
          "1.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, a + 10} end)
          ]
        )

      # Start executions
      exec_included = Journey.start_execution(graph_included)
      exec_excluded = Journey.start_execution(graph_excluded)

      # Build graph tuples explicitly - only include graph_included
      graph_tuples = [{graph_included.name, "1.0"}]

      # Query with excluded execution's id
      query = Helpers.executions_for_graphs(exec_excluded.id, graph_tuples)
      executions = Journey.Repo.all(query)

      # Should return empty because the execution's graph is not in the list
      assert executions == []

      # But querying with included execution's id should work
      query_included = Helpers.executions_for_graphs(exec_included.id, graph_tuples)
      executions_included = Journey.Repo.all(query_included)

      assert length(executions_included) == 1
      assert List.first(executions_included).id == exec_included.id
    end
  end
end
