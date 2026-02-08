defmodule Journey.Graph.CatalogTest do
  use ExUnit.Case, async: false

  alias Journey.Graph.Catalog
  import Journey.Helpers.Random, only: [random_string: 0]

  setup do
    # Generate unique test-specific prefix to avoid collisions with other tests
    test_id = random_string()
    {:ok, test_id: test_id}
  end

  describe "register/1" do
    test "stores graph with composite key", %{test_id: test_id} do
      name = "catalog_reg_#{test_id}"
      graph = Journey.new_graph(name, "1.0.0", [])

      result = Catalog.register(graph)

      assert result == graph
      assert Catalog.fetch(name, "1.0.0") == graph
    end

    test "allows multiple versions of same graph", %{test_id: test_id} do
      name = "catalog_versions_#{test_id}"
      graph_v1 = Journey.new_graph(name, "1.0.0", [])
      graph_v2 = Journey.new_graph(name, "2.0.0", [])

      Catalog.register(graph_v1)
      Catalog.register(graph_v2)

      assert Catalog.fetch(name, "1.0.0") == graph_v1
      assert Catalog.fetch(name, "2.0.0") == graph_v2
    end

    test "silently overwrites existing {name, version}", %{test_id: test_id} do
      name = "catalog_overwrite_#{test_id}"
      graph1 = Journey.new_graph(name, "1.0.0", [])
      graph2 = Journey.new_graph(name, "1.0.0", [])

      Catalog.register(graph1)
      Catalog.register(graph2)

      assert Catalog.fetch(name, "1.0.0") == graph2
    end
  end

  describe "fetch/2" do
    test "returns graph for valid name and version", %{test_id: test_id} do
      name = "catalog_fetch_#{test_id}"
      graph = Journey.new_graph(name, "1.0.0", [])
      Catalog.register(graph)

      assert Catalog.fetch(name, "1.0.0") == graph
    end

    test "returns nil for unknown name" do
      assert Catalog.fetch("unknown_#{random_string()}", "1.0.0") == nil
    end

    test "returns nil for unknown version", %{test_id: test_id} do
      name = "catalog_fetch_nil_#{test_id}"
      graph = Journey.new_graph(name, "1.0.0", [])
      Catalog.register(graph)

      assert Catalog.fetch(name, "2.0.0") == nil
    end

    test "requires both parameters", %{test_id: test_id} do
      name = "catalog_fetch_both_#{test_id}"
      graph = Journey.new_graph(name, "1.0.0", [])
      Catalog.register(graph)

      assert Catalog.fetch(name, "1.0.0") == graph
    end
  end

  describe "list/2" do
    setup %{test_id: test_id} do
      name1 = "catalog_list_g1_#{test_id}"
      name2 = "catalog_list_g2_#{test_id}"

      graph1_v1 = Journey.new_graph(name1, "1.0.0", [])
      graph1_v2 = Journey.new_graph(name1, "2.0.0", [])
      graph2_v1 = Journey.new_graph(name2, "1.0.0", [])

      Catalog.register(graph1_v1)
      Catalog.register(graph1_v2)
      Catalog.register(graph2_v1)

      %{
        name1: name1,
        name2: name2,
        graph1_v1: graph1_v1,
        graph1_v2: graph1_v2,
        graph2_v1: graph2_v1
      }
    end

    test "list() returns all graphs including ours", ctx do
      all_graphs = Catalog.list()

      assert ctx.graph1_v1 in all_graphs
      assert ctx.graph1_v2 in all_graphs
      assert ctx.graph2_v1 in all_graphs
    end

    test "list(nil, nil) returns all graphs including ours", ctx do
      all_graphs = Catalog.list(nil, nil)

      assert ctx.graph1_v1 in all_graphs
      assert ctx.graph1_v2 in all_graphs
      assert ctx.graph2_v1 in all_graphs
    end

    test "list(name) returns all versions of a graph sorted descending", ctx do
      versions = Catalog.list(ctx.name1)

      assert length(versions) == 2
      # 2.0.0 should come before 1.0.0
      assert versions == [ctx.graph1_v2, ctx.graph1_v1]
    end

    test "list(name, nil) returns all versions of a graph", ctx do
      versions = Catalog.list(ctx.name1, nil)

      assert length(versions) == 2
      assert versions == [ctx.graph1_v2, ctx.graph1_v1]
    end

    test "list(name, version) returns specific version as a list", ctx do
      result = Catalog.list(ctx.name1, "1.0.0")

      assert result == [ctx.graph1_v1]
    end

    test "list(name, version) returns empty list for unknown graph" do
      result = Catalog.list("unknown_#{random_string()}", "1.0.0")

      assert result == []
    end

    test "list(name, version) returns empty list for unknown version", ctx do
      result = Catalog.list(ctx.name1, "999.0.0")

      assert result == []
    end

    test "list(name) returns empty list for unknown graph" do
      result = Catalog.list("unknown_#{random_string()}")

      assert result == []
    end

    test "list(nil, version) raises ArgumentError" do
      assert_raise ArgumentError, "graph_version cannot be specified without graph_name", fn ->
        Catalog.list(nil, "1.0.0")
      end
    end
  end

  describe "unregister/2" do
    test "removes registered graph", %{test_id: test_id} do
      name = "catalog_unreg_#{test_id}"
      graph = Journey.new_graph(name, "1.0.0", [])
      Catalog.register(graph)

      assert Catalog.fetch(name, "1.0.0") == graph

      result = Catalog.unregister(name, "1.0.0")

      assert result == :ok
      assert Catalog.fetch(name, "1.0.0") == nil
    end

    test "removes specific version while leaving others intact", %{test_id: test_id} do
      name = "catalog_unreg_ver_#{test_id}"
      graph_v1 = Journey.new_graph(name, "1.0.0", [])
      graph_v2 = Journey.new_graph(name, "2.0.0", [])

      Catalog.register(graph_v1)
      Catalog.register(graph_v2)

      result = Catalog.unregister(name, "1.0.0")

      assert result == :ok
      assert Catalog.fetch(name, "1.0.0") == nil
      assert Catalog.fetch(name, "2.0.0") == graph_v2
    end

    test "succeeds silently for unknown graph" do
      result = Catalog.unregister("unknown_#{random_string()}", "1.0.0")

      assert result == :ok
    end

    test "succeeds silently for unknown version", %{test_id: test_id} do
      name = "catalog_unreg_unkn_#{test_id}"
      graph = Journey.new_graph(name, "1.0.0", [])
      Catalog.register(graph)

      result = Catalog.unregister(name, "2.0.0")

      assert result == :ok
      assert Catalog.fetch(name, "1.0.0") == graph
    end

    test "does not affect other graphs", %{test_id: test_id} do
      name1 = "catalog_unreg_g1_#{test_id}"
      name2 = "catalog_unreg_g2_#{test_id}"
      graph1 = Journey.new_graph(name1, "1.0.0", [])
      graph2 = Journey.new_graph(name2, "1.0.0", [])

      Catalog.register(graph1)
      Catalog.register(graph2)

      result = Catalog.unregister(name1, "1.0.0")

      assert result == :ok
      assert Catalog.fetch(name1, "1.0.0") == nil
      assert Catalog.fetch(name2, "1.0.0") == graph2
    end

    test "unregistered graph does not appear in list", %{test_id: test_id} do
      name = "catalog_unreg_list_#{test_id}"
      graph1 = Journey.new_graph(name, "1.0.0", [])
      graph2 = Journey.new_graph(name, "2.0.0", [])

      Catalog.register(graph1)
      Catalog.register(graph2)

      assert length(Catalog.list(name)) == 2

      Catalog.unregister(name, "1.0.0")

      remaining = Catalog.list(name)
      assert length(remaining) == 1
      assert remaining == [graph2]
    end

    test "unregistering all versions results in empty list for graph name", %{test_id: test_id} do
      name = "catalog_unreg_all_#{test_id}"
      graph1 = Journey.new_graph(name, "1.0.0", [])
      graph2 = Journey.new_graph(name, "2.0.0", [])

      Catalog.register(graph1)
      Catalog.register(graph2)

      Catalog.unregister(name, "1.0.0")
      Catalog.unregister(name, "2.0.0")

      assert Catalog.list(name) == []
    end

    test "requires both parameters", %{test_id: test_id} do
      name = "catalog_unreg_both_#{test_id}"
      graph = Journey.new_graph(name, "1.0.0", [])
      Catalog.register(graph)

      result = Catalog.unregister(name, "1.0.0")

      assert result == :ok
    end
  end

  describe "version sorting" do
    test "sorts versions in descending order", %{test_id: test_id} do
      name = "catalog_sort_#{test_id}"
      # Test with various version formats to ensure string sorting works correctly
      versions = ["1.0.0", "1.0.1", "1.1.0", "2.0.0", "10.0.0"]

      for version <- versions do
        graph = Journey.new_graph(name, version, [])
        Catalog.register(graph)
      end

      result = Catalog.list(name)
      result_versions = Enum.map(result, & &1.version)

      # String sorting will put them in this order (descending)
      expected_order = ["2.0.0", "10.0.0", "1.1.0", "1.0.1", "1.0.0"]
      assert result_versions == expected_order
    end
  end
end
