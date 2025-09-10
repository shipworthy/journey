defmodule Journey.Scheduler.Background.Sweeps.Helpers.ExecutionsForGraphsTest do
  use ExUnit.Case, async: false
  import Journey.Node
  import Journey.Helpers.Random
  alias Journey.Graph.Catalog
  alias Journey.Scheduler.Background.Sweeps.Helpers

  describe "executions_for_graphs/2" do
    test "filters executions by registered graphs only" do
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

      # Get all registered graphs before unregistering
      all_graphs = Catalog.list()
      assert length(all_graphs) >= 3

      # Unregister graph_b
      :ok = Catalog.unregister(graph_b.name, "1.0")

      # Get remaining registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Verify graph_b is not in registered graphs
      refute {graph_b.name, "1.0"} in registered_graph_tuples
      assert {graph_a.name, "1.0"} in registered_graph_tuples
      assert {graph_c.name, "1.0"} in registered_graph_tuples

      # Query executions for registered graphs only
      query = Helpers.executions_for_graphs(nil, registered_graph_tuples)
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

      # Unregister v1.0
      :ok = Catalog.unregister(graph_name, "1.0")

      # Get remaining registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query should only return executions from v2.0
      query = Helpers.executions_for_graphs(nil, registered_graph_tuples)
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

      # Get registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query executions - should only include non-archived
      query = Helpers.executions_for_graphs(nil, registered_graph_tuples)
      executions = Journey.Repo.all(query)

      execution_ids = Enum.map(executions, & &1.id)

      # Only non-archived execution should be in the results
      refute exec_1.id in execution_ids
      assert exec_2.id in execution_ids
    end
  end

  describe "executions_for_graphs/2 with execution_id" do
    test "filters specific execution when graph is registered" do
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

      # Get all registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with specific execution_id
      query = Helpers.executions_for_graphs(exec_a.id, registered_graph_tuples)
      executions = Journey.Repo.all(query)

      # Should only return exec_a
      assert length(executions) == 1
      assert List.first(executions).id == exec_a.id
    end

    test "returns empty list when execution's graph is not in the registered list" do
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

      # Unregister the graph
      :ok = Catalog.unregister(graph.name, graph.version)

      # Get remaining registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with execution_id but graph is not registered
      query = Helpers.executions_for_graphs(execution.id, registered_graph_tuples)
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

    test "filters correctly when multiple executions exist for the same registered graph" do
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

      # Get registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with exec_1's id
      query = Helpers.executions_for_graphs(exec_1.id, registered_graph_tuples)
      executions = Journey.Repo.all(query)

      # Should only return exec_1
      assert length(executions) == 1
      assert List.first(executions).id == exec_1.id
    end

    test "returns empty list for non-existent execution_id" do
      # Create a graph to ensure there are registered graphs
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

      # Get registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with non-existent execution_id
      fake_execution_id = "EXEC_FAKE_#{random_string()}"
      query = Helpers.executions_for_graphs(fake_execution_id, registered_graph_tuples)
      executions = Journey.Repo.all(query)

      # Should return empty
      assert executions == []
    end

    test "correctly handles execution_id that exists but doesn't match any registered graphs" do
      # Create two graphs with unique names
      graph_registered =
        Journey.new_graph(
          "exec_registered_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      graph_unregistered =
        Journey.new_graph(
          "exec_will_unregister_#{random_string()}",
          "1.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, a + 10} end)
          ]
        )

      # Start executions
      exec_registered = Journey.start_execution(graph_registered)
      exec_unregistered = Journey.start_execution(graph_unregistered)

      # Unregister the second graph
      :ok = Catalog.unregister(graph_unregistered.name, graph_unregistered.version)

      # Get remaining registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with unregistered execution's id
      query = Helpers.executions_for_graphs(exec_unregistered.id, registered_graph_tuples)
      executions = Journey.Repo.all(query)

      # Should return empty because the execution's graph is not registered
      assert executions == []

      # But querying with registered execution's id should work
      query_registered = Helpers.executions_for_graphs(exec_registered.id, registered_graph_tuples)
      executions_registered = Journey.Repo.all(query_registered)

      assert length(executions_registered) == 1
      assert List.first(executions_registered).id == exec_registered.id
    end
  end
end
