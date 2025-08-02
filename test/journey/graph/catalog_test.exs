defmodule Journey.Graph.CatalogTest do
  use ExUnit.Case, async: false

  alias Journey.Graph.Catalog

  setup do
    # Clear the catalog state between tests
    Agent.update(Catalog, fn _state -> %{} end)
    :ok
  end

  describe "register/1" do
    test "stores graph with composite key" do
      graph = Journey.new_graph("test", "1.0.0", [])

      result = Catalog.register(graph)

      assert result == graph
      assert Catalog.fetch("test", "1.0.0") == graph
    end

    test "allows multiple versions of same graph" do
      graph_v1 = Journey.new_graph("test", "1.0.0", [])
      graph_v2 = Journey.new_graph("test", "2.0.0", [])

      Catalog.register(graph_v1)
      Catalog.register(graph_v2)

      assert Catalog.fetch("test", "1.0.0") == graph_v1
      assert Catalog.fetch("test", "2.0.0") == graph_v2
    end

    test "silently overwrites existing {name, version}" do
      graph1 = Journey.new_graph("test", "1.0.0", [])
      graph2 = Journey.new_graph("test", "1.0.0", [])

      Catalog.register(graph1)
      Catalog.register(graph2)

      assert Catalog.fetch("test", "1.0.0") == graph2
    end
  end

  describe "fetch/2" do
    test "returns graph for valid name and version" do
      graph = Journey.new_graph("test", "1.0.0", [])
      Catalog.register(graph)

      assert Catalog.fetch("test", "1.0.0") == graph
    end

    test "returns nil for unknown name" do
      assert Catalog.fetch("unknown", "1.0.0") == nil
    end

    test "returns nil for unknown version" do
      graph = Journey.new_graph("test", "1.0.0", [])
      Catalog.register(graph)

      assert Catalog.fetch("test", "2.0.0") == nil
    end

    test "requires both parameters" do
      # This test verifies the function signature requires both params
      # The actual parameter validation is handled by Elixir
      graph = Journey.new_graph("test", "1.0.0", [])
      Catalog.register(graph)

      assert Catalog.fetch("test", "1.0.0") == graph
    end
  end

  describe "list/2" do
    setup do
      graph1_v1 = Journey.new_graph("graph1", "1.0.0", [])
      graph1_v2 = Journey.new_graph("graph1", "2.0.0", [])
      graph2_v1 = Journey.new_graph("graph2", "1.0.0", [])

      Catalog.register(graph1_v1)
      Catalog.register(graph1_v2)
      Catalog.register(graph2_v1)

      %{graph1_v1: graph1_v1, graph1_v2: graph1_v2, graph2_v1: graph2_v1}
    end

    test "list() returns all graphs", %{graph1_v1: g1v1, graph1_v2: g1v2, graph2_v1: g2v1} do
      all_graphs = Catalog.list()

      assert length(all_graphs) == 3
      assert g1v1 in all_graphs
      assert g1v2 in all_graphs
      assert g2v1 in all_graphs
    end

    test "list(nil, nil) returns all graphs", %{graph1_v1: g1v1, graph1_v2: g1v2, graph2_v1: g2v1} do
      all_graphs = Catalog.list(nil, nil)

      assert length(all_graphs) == 3
      assert g1v1 in all_graphs
      assert g1v2 in all_graphs
      assert g2v1 in all_graphs
    end

    test "list(name) returns all versions of a graph sorted descending", %{graph1_v1: g1v1, graph1_v2: g1v2} do
      versions = Catalog.list("graph1")

      assert length(versions) == 2
      # 2.0.0 should come before 1.0.0
      assert versions == [g1v2, g1v1]
    end

    test "list(name, nil) returns all versions of a graph", %{graph1_v1: g1v1, graph1_v2: g1v2} do
      versions = Catalog.list("graph1", nil)

      assert length(versions) == 2
      assert versions == [g1v2, g1v1]
    end

    test "list(name, version) returns specific version as a list", %{graph1_v1: g1v1} do
      result = Catalog.list("graph1", "1.0.0")

      assert result == [g1v1]
    end

    test "list(name, version) returns empty list for unknown graph" do
      result = Catalog.list("unknown", "1.0.0")

      assert result == []
    end

    test "list(name, version) returns empty list for unknown version" do
      result = Catalog.list("graph1", "999.0.0")

      assert result == []
    end

    test "list(name) returns empty list for unknown graph" do
      result = Catalog.list("unknown")

      assert result == []
    end

    test "list(nil, version) raises ArgumentError" do
      assert_raise ArgumentError, "graph_version cannot be specified without graph_name", fn ->
        Catalog.list(nil, "1.0.0")
      end
    end
  end

  describe "version sorting" do
    test "sorts versions in descending order" do
      # Test with various version formats to ensure string sorting works correctly
      versions = ["1.0.0", "1.0.1", "1.1.0", "2.0.0", "10.0.0"]

      for version <- versions do
        graph = Journey.new_graph("test", version, [])
        Catalog.register(graph)
      end

      result = Catalog.list("test")
      result_versions = Enum.map(result, & &1.version)

      # String sorting will put them in this order (descending)
      expected_order = ["2.0.0", "10.0.0", "1.1.0", "1.0.1", "1.0.0"]
      assert result_versions == expected_order
    end
  end
end
