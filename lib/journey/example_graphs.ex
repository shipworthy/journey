defmodule Journey.ExampleGraphs do
  use Journey.GraphRegistry
  import Journey.Node

  @registered_graph true
  def greeting_graph() do
    Journey.new_graph(
      "greeting graph",
      "v1.0.0",
      [
        input(:name),
        compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello #{name}"} end)
      ]
    )
  end

  @registered_graph true
  def math_graph() do
    Journey.new_graph(
      "simple math graph",
      "v1.0.0",
      [
        input(:x),
        input(:y),
        compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end),
        compute(:product, [:x, :y], fn %{x: x, y: y} -> {:ok, x * y} end)
      ]
    )
  end

  def not_registered_graph() do
    Journey.new_graph(
      "not registered",
      "v1.0.0",
      [input(:value)]
    )
  end
end
