defmodule Journey.NewGraphApiTest do
  use ExUnit.Case, async: true
  import Journey.Node
  import Journey.Helpers.Random, only: [random_string: 0]

  describe "new_graph/1 with nodes only" do
    test "creates graph with auto-generated name and default version" do
      graph = Journey.new_graph([input(:test_node)])

      assert String.starts_with?(graph.name, "graph_")
      assert String.length(graph.name) == 14
      assert graph.version == "v1.0"
      assert length(graph.nodes) == 3
    end

    test "same nodes produce same graph name (deterministic)" do
      nodes = [
        input(:name),
        compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello #{name}"} end)
      ]

      graph1 = Journey.new_graph(nodes)
      graph2 = Journey.new_graph(nodes)

      assert graph1.name == graph2.name
      assert graph1.hash == graph2.hash
    end

    test "different nodes produce different graph names" do
      graph1 = Journey.new_graph([input(:a)])
      graph2 = Journey.new_graph([input(:b)])

      assert graph1.name != graph2.name
    end
  end

  describe "new_graph/2 with name and nodes" do
    test "creates graph with provided name and default version" do
      name = "MyGraph_#{random_string()}"
      graph = Journey.new_graph(name, [input(:test_node)])

      assert graph.name == name
      assert graph.version == "v1.0"
      assert length(graph.nodes) == 3
    end
  end

  describe "new_graph/3 with name, version, and nodes (existing behavior)" do
    test "creates graph with provided name and version" do
      name = "MyGraph_#{random_string()}"
      graph = Journey.new_graph(name, "v2.0", [input(:test_node)])

      assert graph.name == name
      assert graph.version == "v2.0"
      assert length(graph.nodes) == 3
    end
  end

  describe "start/1 alias" do
    test "works identically to start_execution/1" do
      graph = Journey.new_graph("TestGraph_#{random_string()}", "v1.0", [input(:test)])

      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start(graph)

      assert execution1.graph_name == execution2.graph_name
      assert execution1.graph_version == execution2.graph_version
      assert execution1.id != execution2.id
    end

    test "can be used in a pipeline" do
      graph = Journey.new_graph([input(:name)])

      execution =
        graph
        |> Journey.start()
        |> Journey.set(:name, "Test")

      assert {:ok, "Test", 1} = Journey.get(execution, :name)
    end
  end
end
