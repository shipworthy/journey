defmodule Journey.GraphRegistryTest do
  use ExUnit.Case, async: true

  test "@registered_graph functions are automatically registered in Catalog" do
    # Check that graphs marked with @registered_graph are in the catalog
    assert %Journey.Graph{} = Journey.Graph.Catalog.fetch("greeting graph", "v1.0.0")
    assert %Journey.Graph{} = Journey.Graph.Catalog.fetch("simple math graph", "v1.0.0")

    # Check that the unmarked graph is NOT in the catalog
    assert nil == Journey.Graph.Catalog.fetch("not registered", "v1.0.0")
  end

  test "Journey.ExampleGraphs exports __registered_graphs__" do
    assert function_exported?(Journey.ExampleGraphs, :__registered_graphs__, 0)
    registered = Journey.ExampleGraphs.__registered_graphs__()

    assert {:greeting_graph, 0} in registered
    assert {:math_graph, 0} in registered
    refute {:not_registered_graph, 0} in registered
  end

  test "registered graphs from Catalog are functional" do
    # Fetch and use the greeting graph from the catalog
    greeting_graph = Journey.Graph.Catalog.fetch("greeting graph", "v1.0.0")
    assert greeting_graph != nil

    exec = Journey.start_execution(greeting_graph)
    exec = Journey.set_value(exec, :name, "Bob")
    {:ok, greeting} = Journey.get_value(exec, :greeting, wait_new: true)

    assert greeting == "Hello Bob"
  end

  test "math graph from Catalog works correctly" do
    math_graph = Journey.Graph.Catalog.fetch("simple math graph", "v1.0.0")
    assert math_graph != nil

    exec = Journey.start_execution(math_graph)
    exec = Journey.set_value(exec, :x, 5)
    exec = Journey.set_value(exec, :y, 3)

    {:ok, sum} = Journey.get_value(exec, :sum, wait_new: true)
    {:ok, product} = Journey.get_value(exec, :product, wait_new: true)

    assert sum == 8
    assert product == 15
  end
end
