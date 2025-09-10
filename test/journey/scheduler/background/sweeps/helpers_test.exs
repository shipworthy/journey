defmodule Journey.Scheduler.Background.Sweeps.HelpersTest do
  use ExUnit.Case, async: false
  import Journey.Node
  import Journey.Helpers.Random
  alias Journey.Graph.Catalog
  alias Journey.Scheduler.Background.Sweeps.Helpers

  describe "computations_for_graphs/2" do
    test "filters computations by registered graphs only" do
      # Define three different graphs with unique names
      graph_a =
        Journey.new_graph(
          "graph_a_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      graph_b =
        Journey.new_graph(
          "graph_b_#{random_string()}",
          "1.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, a + 10} end)
          ]
        )

      graph_c =
        Journey.new_graph(
          "graph_c_#{random_string()}",
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

      # Set values to trigger computations
      Journey.set_value(exec_a, :x, 5)
      Journey.set_value(exec_b, :a, 3)
      Journey.set_value(exec_c, :p, 4)

      # Wait for computations to complete
      {:ok, _} = Journey.get_value(exec_a, :y, wait_any: true)
      {:ok, _} = Journey.get_value(exec_b, :b, wait_any: true)
      {:ok, _} = Journey.get_value(exec_c, :q, wait_any: true)

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

      # Query computations for registered graphs only
      query = Helpers.computations_for_graphs(nil, registered_graph_tuples)
      computations = Journey.Repo.all(query)

      # Verify we only get computations from graph_a and graph_c
      computation_execution_ids = Enum.map(computations, & &1.execution_id) |> Enum.uniq()

      assert exec_a.id in computation_execution_ids
      assert exec_c.id in computation_execution_ids
      refute exec_b.id in computation_execution_ids

      # Verify computation count (each graph has 1 compute node)
      exec_a_computations = Enum.filter(computations, &(&1.execution_id == exec_a.id))
      exec_c_computations = Enum.filter(computations, &(&1.execution_id == exec_c.id))

      assert length(exec_a_computations) == 1
      assert length(exec_c_computations) == 1
    end

    test "returns empty list when no graphs match" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "test_graph_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution
      execution = Journey.start_execution(graph)
      Journey.set_value(execution, :x, 5)
      {:ok, _} = Journey.get_value(execution, :y, wait_any: true)

      # Query with non-existent graph
      query = Helpers.computations_for_graphs(nil, [{"non_existent", "1.0"}])
      computations = Journey.Repo.all(query)

      assert computations == []
    end

    test "handles multiple versions of same graph" do
      # Create two versions of the same graph with unique name
      graph_name = "versioned_graph_#{random_string()}"

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

      Journey.set_value(exec_v1, :x, 5)
      Journey.set_value(exec_v2, :x, 5)

      {:ok, _} = Journey.get_value(exec_v1, :y, wait_any: true)
      {:ok, _} = Journey.get_value(exec_v2, :y, wait_any: true)

      # Unregister v1.0
      :ok = Catalog.unregister(graph_name, "1.0")

      # Get remaining registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query should only return computations from v2.0
      query = Helpers.computations_for_graphs(nil, registered_graph_tuples)
      computations = Journey.Repo.all(query)

      computation_execution_ids = Enum.map(computations, & &1.execution_id) |> Enum.uniq()

      refute exec_v1.id in computation_execution_ids
      assert exec_v2.id in computation_execution_ids
    end

    test "returns empty list when called with empty graph list" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "test_graph_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution and trigger computation
      execution = Journey.start_execution(graph)
      Journey.set_value(execution, :x, 5)
      {:ok, _} = Journey.get_value(execution, :y, wait_any: true)

      # Query with empty graph list
      query = Helpers.computations_for_graphs(nil, [])
      computations = Journey.Repo.all(query)

      # Should return no computations even though computations exist
      assert computations == []
    end
  end

  describe "computations_for_graphs/2 with execution_id" do
    test "filters computations for specific execution when graph is registered" do
      # Create multiple graphs with unique names
      graph_a =
        Journey.new_graph(
          "graph_a_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      graph_b =
        Journey.new_graph(
          "graph_b_#{random_string()}",
          "1.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, a + 10} end)
          ]
        )

      # Start executions for each graph
      exec_a = Journey.start_execution(graph_a)
      exec_b = Journey.start_execution(graph_b)

      # Set values to trigger computations
      Journey.set_value(exec_a, :x, 5)
      Journey.set_value(exec_b, :a, 3)

      # Wait for computations to complete
      {:ok, _} = Journey.get_value(exec_a, :y, wait_any: true)
      {:ok, _} = Journey.get_value(exec_b, :b, wait_any: true)

      # Get all registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with specific execution_id
      query = Helpers.computations_for_graphs(exec_a.id, registered_graph_tuples)
      computations = Journey.Repo.all(query)

      # Should only return computations from exec_a
      assert length(computations) == 1
      assert Enum.all?(computations, &(&1.execution_id == exec_a.id))
    end

    test "returns empty list when execution's graph is not in the registered list" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "graph_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution and trigger computation
      execution = Journey.start_execution(graph)
      Journey.set_value(execution, :x, 5)
      {:ok, _} = Journey.get_value(execution, :y, wait_any: true)

      # Unregister the graph
      :ok = Catalog.unregister(graph.name, graph.version)

      # Get remaining registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with execution_id but graph is not registered
      query = Helpers.computations_for_graphs(execution.id, registered_graph_tuples)
      computations = Journey.Repo.all(query)

      # Should return empty even though execution exists
      assert computations == []
    end

    test "returns empty list when called with empty graph list" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "graph_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution and trigger computation
      execution = Journey.start_execution(graph)
      Journey.set_value(execution, :x, 5)
      {:ok, _} = Journey.get_value(execution, :y, wait_any: true)

      # Query with execution_id and empty graph list
      query = Helpers.computations_for_graphs(execution.id, [])
      computations = Journey.Repo.all(query)

      # Should return empty
      assert computations == []
    end

    test "filters correctly when multiple executions exist for the same registered graph" do
      # Create a graph with unique name
      graph =
        Journey.new_graph(
          "graph_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end),
            compute(:z, [:y], fn %{y: y} -> {:ok, y + 1} end)
          ]
        )

      # Start two executions from the same graph
      exec_1 = Journey.start_execution(graph)
      exec_2 = Journey.start_execution(graph)

      # Trigger computations for both
      Journey.set_value(exec_1, :x, 5)
      Journey.set_value(exec_2, :x, 10)

      {:ok, _} = Journey.get_value(exec_1, :z, wait_any: true)
      {:ok, _} = Journey.get_value(exec_2, :z, wait_any: true)

      # Get registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with exec_1's id
      query = Helpers.computations_for_graphs(exec_1.id, registered_graph_tuples)
      computations = Journey.Repo.all(query)

      # Should only return computations from exec_1 (2 compute nodes)
      assert length(computations) == 2
      assert Enum.all?(computations, &(&1.execution_id == exec_1.id))
    end

    test "returns empty list for non-existent execution_id" do
      # Create a graph to ensure there are registered graphs
      graph =
        Journey.new_graph(
          "graph_#{random_string()}",
          "1.0",
          [
            input(:x),
            compute(:y, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      # Start execution to ensure there are computations in the system
      execution = Journey.start_execution(graph)
      Journey.set_value(execution, :x, 5)
      {:ok, _} = Journey.get_value(execution, :y, wait_any: true)

      # Get registered graphs
      registered_graphs = Catalog.list()
      registered_graph_tuples = Enum.map(registered_graphs, fn g -> {g.name, g.version} end)

      # Query with non-existent execution_id
      fake_execution_id = "EXEC_FAKE_#{random_string()}"
      query = Helpers.computations_for_graphs(fake_execution_id, registered_graph_tuples)
      computations = Journey.Repo.all(query)

      # Should return empty
      assert computations == []
    end
  end
end
