defmodule Journey.Persistence.InMemoryTest do
  use ExUnit.Case, async: false
  import Journey.Node

  alias Journey.Persistence.InMemory

  setup do
    # Clear the in-memory store between tests
    InMemory.clear()
    :ok
  end

  describe "store/1" do
    test "stores execution and returns it" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution = Journey.start_execution(graph)

      result = InMemory.store(execution)

      assert result == execution
      assert InMemory.fetch(execution.id) == execution
    end

    test "allows updating existing execution" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution = Journey.start_execution(graph)
      updated_execution = Journey.set(execution, :name, "Alice")

      InMemory.store(execution)
      result = InMemory.store(updated_execution)

      assert result == updated_execution
      assert InMemory.fetch(execution.id) == updated_execution
    end

    test "stores multiple executions independently" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)

      InMemory.store(execution1)
      InMemory.store(execution2)

      assert InMemory.fetch(execution1.id) == execution1
      assert InMemory.fetch(execution2.id) == execution2
      assert execution1.id != execution2.id
    end
  end

  describe "fetch/1" do
    test "returns execution for valid ID" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution = Journey.start_execution(graph)
      InMemory.store(execution)

      result = InMemory.fetch(execution.id)

      assert result == execution
    end

    test "returns nil for unknown ID" do
      result = InMemory.fetch("unknown-id")

      assert result == nil
    end

    test "returns nil for non-string ID" do
      # This test verifies the function clause requires string IDs
      assert_raise FunctionClauseError, fn ->
        InMemory.fetch(123)
      end
    end
  end

  describe "delete/1" do
    test "removes stored execution" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution = Journey.start_execution(graph)
      InMemory.store(execution)

      assert InMemory.fetch(execution.id) == execution

      result = InMemory.delete(execution.id)

      assert result == :ok
      assert InMemory.fetch(execution.id) == nil
    end

    test "removes specific execution while leaving others intact" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)

      InMemory.store(execution1)
      InMemory.store(execution2)

      result = InMemory.delete(execution1.id)

      assert result == :ok
      assert InMemory.fetch(execution1.id) == nil
      assert InMemory.fetch(execution2.id) == execution2
    end

    test "succeeds silently for unknown ID" do
      result = InMemory.delete("unknown-id")

      assert result == :ok
    end

    test "does not affect other executions" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)

      InMemory.store(execution1)
      InMemory.store(execution2)

      result = InMemory.delete(execution1.id)

      assert result == :ok
      assert InMemory.fetch(execution2.id) == execution2
    end
  end

  describe "list/0" do
    test "returns empty list when no executions stored" do
      result = InMemory.list()

      assert result == []
    end

    test "returns all stored executions" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)
      execution3 = Journey.start_execution(graph)

      InMemory.store(execution1)
      InMemory.store(execution2)
      InMemory.store(execution3)

      result = InMemory.list()

      assert length(result) == 3
      assert execution1 in result
      assert execution2 in result
      assert execution3 in result
    end

    test "reflects changes after deletions" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)

      InMemory.store(execution1)
      InMemory.store(execution2)

      assert length(InMemory.list()) == 2

      InMemory.delete(execution1.id)

      remaining = InMemory.list()
      assert length(remaining) == 1
      assert remaining == [execution2]
    end
  end

  describe "clear/0" do
    test "removes all stored executions" do
      graph = Journey.new_graph("test", "1.0.0", [input(:name)])
      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)

      InMemory.store(execution1)
      InMemory.store(execution2)

      assert length(InMemory.list()) == 2

      result = InMemory.clear()

      assert result == :ok
      assert InMemory.list() == []
      assert InMemory.fetch(execution1.id) == nil
      assert InMemory.fetch(execution2.id) == nil
    end

    test "succeeds when store is already empty" do
      result = InMemory.clear()

      assert result == :ok
      assert InMemory.list() == []
    end
  end

  describe "integration with Journey workflow" do
    test "stores and retrieves execution with values" do
      # Create a graph with input and compute nodes
      graph =
        Journey.new_graph(
          "integration test",
          "1.0.0",
          [
            input(:name),
            input(:age),
            compute(
              :greeting,
              [:name],
              fn %{name: name} -> {:ok, "Hello, #{name}!"} end
            )
          ]
        )

      # Create execution and set values
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :name, "Alice")
      execution = Journey.set(execution, :age, 30)

      # Store in memory
      InMemory.store(execution)

      # Retrieve and verify
      fetched_execution = InMemory.fetch(execution.id)
      assert fetched_execution.id == execution.id
      assert fetched_execution.graph_name == "integration test"
      assert fetched_execution.graph_version == "1.0.0"

      # Verify values are preserved
      values = Journey.values(fetched_execution, reload: false)
      assert values[:name] == "Alice"
      assert values[:age] == 30
    end

    test "handles execution with computed values" do
      # Create a graph with computation
      graph =
        Journey.new_graph(
          "computation test",
          "1.0.0",
          [
            input(:x),
            input(:y),
            compute(
              :sum,
              [:x, :y],
              fn %{x: x, y: y} -> {:ok, x + y} end
            )
          ]
        )

      # Create execution and set values to trigger computation
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :x, 10)
      execution = Journey.set(execution, :y, 20)

      # Wait for computation to complete
      {:ok, sum} = Journey.get_value(execution, :sum, wait_any: true)
      assert sum == 30

      # Reload to get the latest state
      execution = Journey.load(execution)

      # Store and retrieve
      InMemory.store(execution)
      fetched_execution = InMemory.fetch(execution.id)

      # Verify computed value is preserved
      values = Journey.values(fetched_execution, reload: false)
      assert values[:x] == 10
      assert values[:y] == 20
      assert values[:sum] == 30
    end

    test "maintains execution metadata" do
      graph = Journey.new_graph("metadata test", "2.1.0", [input(:data)])
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :data, "test value")

      # Store and retrieve
      InMemory.store(execution)
      fetched_execution = InMemory.fetch(execution.id)

      # Verify all metadata is preserved
      assert fetched_execution.id == execution.id
      assert fetched_execution.graph_name == execution.graph_name
      assert fetched_execution.graph_version == execution.graph_version
      assert fetched_execution.graph_hash == execution.graph_hash
      assert fetched_execution.revision == execution.revision
      assert fetched_execution.archived_at == execution.archived_at
    end
  end

  describe "error handling" do
    test "store/1 requires execution struct" do
      assert_raise FunctionClauseError, fn ->
        InMemory.store("not an execution")
      end
    end

    test "store/1 requires execution with valid ID" do
      # This would be caught by the guard clause
      invalid_execution = %{id: nil}

      assert_raise FunctionClauseError, fn ->
        InMemory.store(invalid_execution)
      end
    end

    test "fetch/1 requires string ID" do
      assert_raise FunctionClauseError, fn ->
        InMemory.fetch(nil)
      end

      assert_raise FunctionClauseError, fn ->
        InMemory.fetch(123)
      end
    end

    test "delete/1 requires string ID" do
      assert_raise FunctionClauseError, fn ->
        InMemory.delete(nil)
      end

      assert_raise FunctionClauseError, fn ->
        InMemory.delete(123)
      end
    end
  end
end
