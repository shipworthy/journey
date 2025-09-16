defmodule Journey.ExecutionsInMemoryTest do
  use ExUnit.Case, async: false
  import Journey.Node

  alias Journey.Persistence.InMemory
  alias Journey.ExecutionsInMemory

  setup do
    # Clear the in-memory store and set backend to in-memory for these tests
    InMemory.clear()
    original_store = Application.get_env(:journey, :store)
    Application.put_env(:journey, :store, :inmemory)

    # Register a test graph in the catalog
    test_graph =
      Journey.new_graph(
        "test",
        "1.0.0",
        [input(:name), input(:age)]
      )

    Journey.Graph.Catalog.register(test_graph)

    on_exit(fn ->
      InMemory.clear()
      Application.put_env(:journey, :store, original_store)
    end)

    :ok
  end

  describe "create_new/4" do
    test "creates a new execution with graph metadata" do
      graph_name = "test_graph"
      graph_version = "1.0.0"
      graph_hash = "abc123"
      nodes = [input(:name), input(:age)]

      execution = ExecutionsInMemory.create_new(graph_name, graph_version, nodes, graph_hash)

      assert execution.graph_name == graph_name
      assert execution.graph_version == graph_version
      assert execution.graph_hash == graph_hash
      assert execution.revision == 0
      assert is_nil(execution.archived_at)
      assert is_binary(execution.id)
      assert String.starts_with?(execution.id, "EXEC")

      # Verify values were created for each node
      # :name, :age, :execution_id, :last_updated_at
      assert length(execution.values) == 4

      # Check that execution_id and last_updated_at are set
      execution_id_value = Enum.find(execution.values, fn v -> v.node_name == :execution_id end)
      assert execution_id_value.node_value == execution.id
      assert execution_id_value.set_time != nil

      last_updated_value = Enum.find(execution.values, fn v -> v.node_name == :last_updated_at end)
      assert is_integer(last_updated_value.node_value)
      assert last_updated_value.set_time != nil
    end

    test "creates computations for computable nodes" do
      nodes = [
        input(:x),
        compute(:sum, [:x], fn %{x: x} -> {:ok, x + 1} end)
      ]

      execution = ExecutionsInMemory.create_new("test", "1.0.0", nodes, "hash")

      assert length(execution.computations) == 1
      computation = hd(execution.computations)
      assert computation.node_name == :sum
      assert computation.computation_type == :compute
      assert computation.state == :not_set
    end
  end

  describe "load/3" do
    test "loads an existing execution" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      loaded = ExecutionsInMemory.load(execution.id, false, false)

      assert loaded.id == execution.id
      assert loaded.graph_name == "test"
    end

    test "returns nil for non-existent execution" do
      result = ExecutionsInMemory.load("EXEC_NONEXISTENT", false, false)
      assert is_nil(result)
    end

    test "excludes archived executions by default" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")
      ExecutionsInMemory.archive_execution(execution.id)

      # Should return nil when include_archived? is false
      result = ExecutionsInMemory.load(execution.id, false, false)
      assert is_nil(result)

      # Should return execution when include_archived? is true
      result = ExecutionsInMemory.load(execution.id, false, true)
      assert result.id == execution.id
    end

    test "converts node names to atoms when preload is true" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      loaded = ExecutionsInMemory.load(execution.id, true, false)

      # Check that node names are atoms
      name_value = Enum.find(loaded.values, fn v -> v.node_name == :name end)
      assert is_atom(name_value.node_name)
    end
  end

  describe "set_value/3 and get_value/4" do
    test "sets and gets a simple value" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      # Set value by execution struct
      updated_execution = ExecutionsInMemory.set_value(execution, :name, "Alice")
      assert updated_execution.revision == 1

      # Get value back
      {:ok, value} = ExecutionsInMemory.get_value(updated_execution, :name, nil)
      assert value == "Alice"
    end

    test "sets value by execution ID" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      # Set value by execution ID
      updated_execution = ExecutionsInMemory.set_value(execution.id, :name, "Bob")
      assert updated_execution.revision == 1

      {:ok, value} = ExecutionsInMemory.get_value(updated_execution, :name, nil)
      assert value == "Bob"
    end

    test "returns error for unset value" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      result = ExecutionsInMemory.get_value(execution, :name, nil)
      assert result == {:error, :not_set}
    end

    test "handles timeout when waiting for value" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      # Should timeout quickly since value is not set
      result = ExecutionsInMemory.get_value(execution, :name, 100)
      assert result == {:error, :not_set}
    end

    test "wait_new returns newer values" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      # Set initial value
      execution = ExecutionsInMemory.set_value(execution, :name, "Alice")

      # Start task to update value after a delay
      Task.start(fn ->
        Process.sleep(200)
        ExecutionsInMemory.set_value(execution.id, :name, "Bob")
      end)

      # Should get the new value with wait_new
      {:ok, value} = ExecutionsInMemory.get_value(execution, :name, 1000, wait_new: true)
      assert value == "Bob"
    end
  end

  describe "set_values/2" do
    test "sets multiple values atomically" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name), input(:age)], "hash")

      values_map = %{name: "Charlie", age: 25}
      updated_execution = ExecutionsInMemory.set_values(execution, values_map)

      {:ok, name} = ExecutionsInMemory.get_value(updated_execution, :name, nil)
      {:ok, age} = ExecutionsInMemory.get_value(updated_execution, :age, nil)

      assert name == "Charlie"
      assert age == 25
      assert updated_execution.revision == 1
    end

    test "skips unchanged values" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name), input(:age)], "hash")

      # Set initial values
      execution = ExecutionsInMemory.set_values(execution, %{name: "David", age: 30})
      initial_revision = execution.revision

      # Try to set same values again
      execution = ExecutionsInMemory.set_values(execution, %{name: "David", age: 30})

      # Revision should not change
      assert execution.revision == initial_revision
    end
  end

  describe "unset_value/2 and unset_values/2" do
    test "unsets a single value" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      # Set then unset
      execution = ExecutionsInMemory.set_value(execution, :name, "Eve")
      execution = ExecutionsInMemory.unset_value(execution, :name)

      result = ExecutionsInMemory.get_value(execution, :name, nil)
      assert result == {:error, :not_set}
    end

    test "unsets multiple values" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name), input(:age)], "hash")

      # Set both values
      execution = ExecutionsInMemory.set_values(execution, %{name: "Frank", age: 35})

      # Unset both
      execution = ExecutionsInMemory.unset_values(execution, [:name, :age])

      assert ExecutionsInMemory.get_value(execution, :name, nil) == {:error, :not_set}
      assert ExecutionsInMemory.get_value(execution, :age, nil) == {:error, :not_set}
    end

    test "skips already unset values" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      initial_revision = execution.revision

      # Try to unset already unset value
      execution = ExecutionsInMemory.unset_value(execution, :name)

      # Revision should not change
      assert execution.revision == initial_revision
    end
  end

  describe "values/1" do
    test "returns all values with their status" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name), input(:age)], "hash")

      # Set one value
      execution = ExecutionsInMemory.set_value(execution, :name, "Grace")

      values = ExecutionsInMemory.values(execution)

      # Should have all node values including built-ins
      assert Map.has_key?(values, :name)
      assert Map.has_key?(values, :age)
      assert Map.has_key?(values, :execution_id)
      assert Map.has_key?(values, :last_updated_at)

      assert values[:name] == {:set, "Grace"}
      assert values[:age] == :not_set
      assert values[:execution_id] == {:set, execution.id}
      assert {:set, _timestamp} = values[:last_updated_at]
    end
  end

  describe "list/7" do
    test "lists all executions" do
      exec1 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash1")
      exec2 = ExecutionsInMemory.create_new("graph2", "1.0.0", [input(:name)], "hash2")

      executions = ExecutionsInMemory.list(nil, nil, [], [], 100, 0, false)

      assert length(executions) == 2
      ids = Enum.map(executions, & &1.id)
      assert exec1.id in ids
      assert exec2.id in ids
    end

    test "filters by graph name" do
      exec1 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash1")
      _exec2 = ExecutionsInMemory.create_new("graph2", "1.0.0", [input(:name)], "hash2")

      executions = ExecutionsInMemory.list("graph1", nil, [], [], 100, 0, false)

      assert length(executions) == 1
      assert hd(executions).id == exec1.id
    end

    test "filters by graph version" do
      exec1 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash1")
      _exec2 = ExecutionsInMemory.create_new("graph1", "2.0.0", [input(:name)], "hash2")

      executions = ExecutionsInMemory.list("graph1", "1.0.0", [], [], 100, 0, false)

      assert length(executions) == 1
      assert hd(executions).id == exec1.id
    end

    test "excludes archived executions by default" do
      exec1 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash1")
      exec2 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash2")

      # Archive one execution
      ExecutionsInMemory.archive_execution(exec2.id)

      # Should only return non-archived
      executions = ExecutionsInMemory.list("graph1", "1.0.0", [], [], 100, 0, false)
      assert length(executions) == 1
      assert hd(executions).id == exec1.id

      # Should return both when including archived
      executions = ExecutionsInMemory.list("graph1", "1.0.0", [], [], 100, 0, true)
      assert length(executions) == 2
    end

    test "applies pagination" do
      # Create 3 executions
      _exec1 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash1")
      _exec2 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash2")
      _exec3 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash3")

      # Get first 2
      executions = ExecutionsInMemory.list("graph1", "1.0.0", [], [], 2, 0, false)
      assert length(executions) == 2

      # Get third one with offset
      executions = ExecutionsInMemory.list("graph1", "1.0.0", [], [], 2, 2, false)
      assert length(executions) == 1
    end

    test "applies basic filters" do
      exec1 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash1")
      exec2 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash2")

      # Set different values
      ExecutionsInMemory.set_value(exec1, :name, "Alice")
      ExecutionsInMemory.set_value(exec2, :name, "Bob")

      # Filter for Alice
      filters = [{:name, :eq, "Alice"}]
      executions = ExecutionsInMemory.list("graph1", "1.0.0", [], filters, 100, 0, false)

      assert length(executions) == 1
      assert hd(executions).id == exec1.id
    end

    test "applies sorting" do
      exec1 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash1")
      # Ensure different timestamps
      Process.sleep(10)
      exec2 = ExecutionsInMemory.create_new("graph1", "1.0.0", [input(:name)], "hash2")

      # Sort by inserted_at descending (newest first)
      executions = ExecutionsInMemory.list("graph1", "1.0.0", [{:inserted_at, :desc}], [], 100, 0, false)

      assert length(executions) == 2
      # Newer one first
      assert hd(executions).id == exec2.id
      assert Enum.at(executions, 1).id == exec1.id
    end
  end

  describe "archive_execution/1 and unarchive_execution/1" do
    test "archives and unarchives an execution" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      # Archive
      timestamp = ExecutionsInMemory.archive_execution(execution.id)
      assert is_integer(timestamp)

      loaded = ExecutionsInMemory.load(execution.id, false, true)
      assert loaded.archived_at == timestamp

      # Unarchive
      :ok = ExecutionsInMemory.unarchive_execution(execution.id)

      loaded = ExecutionsInMemory.load(execution.id, false, false)
      assert is_nil(loaded.archived_at)
    end

    test "archiving already archived execution returns same timestamp" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      timestamp1 = ExecutionsInMemory.archive_execution(execution.id)
      timestamp2 = ExecutionsInMemory.archive_execution(execution.id)

      assert timestamp1 == timestamp2
    end

    test "unarchiving non-archived execution succeeds" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      result = ExecutionsInMemory.unarchive_execution(execution.id)
      assert result == :ok
    end
  end

  describe "history/1" do
    test "returns chronological history of changes" do
      graph =
        Journey.new_graph(
          "history test",
          "1.0.0",
          [
            input(:x),
            compute(:doubled, [:x], fn %{x: x} -> {:ok, x * 2} end)
          ]
        )

      execution = ExecutionsInMemory.create_new(graph.name, graph.version, graph.nodes, graph.hash)

      # Set a value to trigger computation
      execution = ExecutionsInMemory.set_value(execution, :x, 5)

      # Wait for computation to complete
      {:ok, 10} = ExecutionsInMemory.get_value(execution, :doubled, 5000)

      # Get history
      history = ExecutionsInMemory.history(execution.id)

      # Should contain both the value setting and computation
      assert length(history) >= 2

      # Find the value and computation entries
      value_entry = Enum.find(history, fn h -> h.node_name == :x end)
      compute_entry = Enum.find(history, fn h -> h.node_name == :doubled end)

      assert value_entry.computation_or_value == :value
      assert value_entry.value == 5

      assert compute_entry.computation_or_value == :computation
      assert compute_entry.node_type == :compute
    end
  end

  describe "utility functions" do
    test "find_value_by_name/2" do
      execution = ExecutionsInMemory.create_new("test", "1.0.0", [input(:name)], "hash")

      execution = ExecutionsInMemory.set_value(execution, :name, "Test Value")

      value = ExecutionsInMemory.find_value_by_name(execution, :name)
      assert value.node_value == "Test Value"
      assert value.node_name == :name
    end

    test "find_computations_by_node_name/2" do
      nodes = [
        input(:x),
        compute(:doubled, [:x], fn %{x: x} -> {:ok, x * 2} end)
      ]

      execution = ExecutionsInMemory.create_new("test", "1.0.0", nodes, "hash")

      computations = ExecutionsInMemory.find_computations_by_node_name(execution, :doubled)
      assert length(computations) == 1
      assert hd(computations).node_name == :doubled
    end

    test "convert_values_to_atoms/2" do
      data = [%{node_name: "test"}, %{node_name: "other"}]

      result = ExecutionsInMemory.convert_values_to_atoms(data, :node_name)

      assert result == [%{node_name: :test}, %{node_name: :other}]
    end

    test "convert_all_keys_to_atoms/1" do
      map = %{"key1" => "value1", "key2" => "value2"}

      result = ExecutionsInMemory.convert_all_keys_to_atoms(map)

      assert result == %{key1: "value1", key2: "value2"}
    end
  end

  describe "integration with Journey main API" do
    test "works with Journey.set and Journey.get_value" do
      graph =
        Journey.new_graph(
          "integration test",
          "1.0.0",
          [input(:name)]
        )

      execution = Journey.start_execution(graph)

      # This should use our in-memory backend
      execution = Journey.set(execution, :name, "Integration Test")
      {:ok, value} = Journey.get_value(execution, :name)

      assert value == "Integration Test"
    end

    test "computations work with in-memory backend" do
      graph =
        Journey.new_graph(
          "computation test",
          "1.0.0",
          [
            input(:a),
            input(:b),
            compute(:sum, [:a, :b], fn %{a: a, b: b} -> {:ok, a + b} end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :a, 10)
      execution = Journey.set(execution, :b, 20)

      # Wait for computation
      {:ok, result} = Journey.get_value(execution, :sum, wait_any: true)
      assert result == 30
    end
  end
end
