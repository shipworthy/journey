defmodule Journey.ExecutionNodeNamesAreAtomsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  setup do
    %{test_id: random_string()}
  end

  describe "node names are atoms" do
    test "start_execution returns execution with atom node names", %{test_id: test_id} do
      graph = graph_with_compute(test_id)
      execution = Journey.start_execution(graph)

      assert Enum.all?(execution.values, fn v -> is_atom(v.node_name) end)
      assert Enum.all?(execution.computations, fn c -> is_atom(c.node_name) end)
    end

    test "find_or_start returns execution with atom node names", %{test_id: test_id} do
      graph = singleton_graph(test_id)
      execution = Journey.find_or_start(graph)

      assert Enum.all?(execution.values, fn v -> is_atom(v.node_name) end)
      assert Enum.all?(execution.computations, fn c -> is_atom(c.node_name) end)
    end

    test "load returns execution with atom node names", %{test_id: test_id} do
      graph = graph_with_compute(test_id)
      execution = Journey.start_execution(graph)
      reloaded = Journey.load(execution.id)

      assert Enum.all?(reloaded.values, fn v -> is_atom(v.node_name) end)
      assert Enum.all?(reloaded.computations, fn c -> is_atom(c.node_name) end)
    end
  end

  defp graph_with_compute(test_id) do
    Journey.new_graph(
      "atom test #{test_id}",
      "1.0.0",
      [
        input(:value),
        compute(:doubled, [:value], fn %{value: v} -> {:ok, v * 2} end)
      ]
    )
  end

  defp singleton_graph(test_id) do
    Journey.new_graph(
      "singleton atom test #{test_id}",
      "1.0.0",
      [
        input(:value),
        compute(:doubled, [:value], fn %{value: v} -> {:ok, v * 2} end)
      ],
      singleton: true
    )
  end
end
