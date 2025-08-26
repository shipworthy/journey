defmodule Journey.ExecutionsMigrationTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Ecto.Query

  describe "migrate_to_current_graph_if_needed/1" do
    test "sunny-day migration: adds new nodes when graph is updated" do
      # Create original graph with two input nodes
      original_graph =
        Journey.new_graph(
          "migration_test_graph",
          "v1",
          [
            input(:name),
            input(:age)
          ]
        )

      # Start execution with original graph
      execution = Journey.start_execution(original_graph)
      execution = Journey.set_value(execution, :name, "Alice")
      execution = Journey.set_value(execution, :age, 30)

      # Verify initial state
      assert {:ok, "Alice"} == Journey.get_value(execution, :name)
      assert {:ok, 30} == Journey.get_value(execution, :age)
      original_hash = execution.graph_hash

      # Create updated graph with additional nodes
      updated_graph =
        Journey.new_graph(
          "migration_test_graph",
          "v1",
          [
            input(:name),
            input(:age),
            input(:email),
            compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello, #{name}!"} end)
          ]
        )

      # Verify the graphs have different hashes
      assert original_hash != updated_graph.hash

      # Perform migration
      migrated_execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)

      # Verify migration results
      assert migrated_execution.id == execution.id
      assert migrated_execution.graph_hash == updated_graph.hash

      # Original values should be preserved
      assert {:ok, "Alice"} == Journey.get_value(migrated_execution, :name)
      assert {:ok, 30} == Journey.get_value(migrated_execution, :age)

      # New input node should exist but be unset
      assert {:error, :not_set} == Journey.get_value(migrated_execution, :email)

      # New compute node should exist but be unset (until triggered)
      assert {:error, :not_set} == Journey.get_value(migrated_execution, :greeting)

      # Verify we can set the new input node
      migrated_execution = Journey.set_value(migrated_execution, :email, "alice@example.com")
      assert {:ok, "alice@example.com"} == Journey.get_value(migrated_execution, :email)

      # Verify the compute node works (it should compute since :name is already set)
      assert {:ok, "Hello, Alice!"} == Journey.get_value(migrated_execution, :greeting, wait_any: true)
    end

    test "migration is idempotent" do
      # Create original graph
      original_graph =
        Journey.new_graph(
          "idempotent_test_graph",
          "v1",
          [input(:a)]
        )

      # Start execution
      execution = Journey.start_execution(original_graph)
      execution = Journey.set_value(execution, :a, "value_a")

      # Create updated graph
      updated_graph =
        Journey.new_graph(
          "idempotent_test_graph",
          "v1",
          [input(:a), input(:b)]
        )

      # First migration
      migrated_once = Journey.Executions.migrate_to_current_graph_if_needed(execution)
      assert migrated_once.graph_hash == updated_graph.hash

      # Second migration (should be no-op)
      migrated_twice = Journey.Executions.migrate_to_current_graph_if_needed(migrated_once)
      assert migrated_twice.graph_hash == updated_graph.hash
      assert migrated_twice.id == migrated_once.id

      # Values should remain the same
      assert {:ok, "value_a"} == Journey.get_value(migrated_twice, :a)
      assert {:error, :not_set} == Journey.get_value(migrated_twice, :b)
    end

    test "no migration when hashes match" do
      # Create graph
      graph =
        Journey.new_graph(
          "no_migration_test",
          "v1",
          [input(:x), input(:y)]
        )

      # Start execution
      execution = Journey.start_execution(graph)
      original_hash = execution.graph_hash

      # Call migrate with same graph (same hash)
      result = Journey.Executions.migrate_to_current_graph_if_needed(execution)

      # Should return same execution object (no migration)
      assert result == execution
      assert result.graph_hash == original_hash
    end

    test "migration handles complex node types" do
      # Original graph with just input
      original_graph =
        Journey.new_graph(
          "complex_migration_test",
          "v1",
          [
            input(:user_id)
          ]
        )

      execution = Journey.start_execution(original_graph)
      execution = Journey.set_value(execution, :user_id, 123)

      # Updated graph with various node types
      _updated_graph =
        Journey.new_graph(
          "complex_migration_test",
          "v1",
          [
            input(:user_id),
            input(:user_name),
            compute(:user_greeting, [:user_name], fn %{user_name: name} -> {:ok, "Welcome, #{name}"} end),
            mutate(
              :clear_user_id,
              [:user_greeting],
              fn _ -> {:ok, nil} end,
              mutates: :user_id
            )
          ]
        )

      # Perform migration
      migrated = Journey.Executions.migrate_to_current_graph_if_needed(execution)

      # Check all nodes exist
      assert {:ok, 123} == Journey.get_value(migrated, :user_id)
      assert {:error, :not_set} == Journey.get_value(migrated, :user_name)
      assert {:error, :not_set} == Journey.get_value(migrated, :user_greeting)
      assert {:error, :not_set} == Journey.get_value(migrated, :clear_user_id)

      # Verify the nodes can be used
      migrated = Journey.set_value(migrated, :user_name, "Bob")
      assert {:ok, "Welcome, Bob"} == Journey.get_value(migrated, :user_greeting, wait_any: true)
    end

    test "new nodes start with ex_revision 0" do
      # Create original graph
      original_graph =
        Journey.new_graph(
          "ex_revision_test",
          "v1",
          [input(:original_node)]
        )

      # Start execution and set some values to increase revision
      execution = Journey.start_execution(original_graph)
      execution = Journey.set_value(execution, :original_node, "value1")
      execution = Journey.set_value(execution, :original_node, "value2")

      # Verify execution has a higher revision
      assert execution.revision > 0

      # Create updated graph with new node
      _updated_graph =
        Journey.new_graph(
          "ex_revision_test",
          "v1",
          [input(:original_node), input(:new_node)]
        )

      # Perform migration
      migrated = Journey.Executions.migrate_to_current_graph_if_needed(execution)

      # Find the new node's value record
      new_node_value = Journey.Executions.find_value_by_name(migrated, :new_node)

      # New node should have ex_revision: 0
      assert new_node_value.ex_revision == 0
      assert new_node_value.set_time == nil
      assert new_node_value.node_value == nil

      # Original node should maintain its revision
      original_node_value = Journey.Executions.find_value_by_name(migrated, :original_node)
      assert original_node_value.ex_revision > 0
    end

    test "concurrent migrations are handled safely" do
      # This test verifies that advisory locks prevent race conditions
      # We can't easily simulate true concurrency in tests, but we can verify
      # that repeated migrations don't cause issues

      original_graph =
        Journey.new_graph(
          "concurrent_test",
          "v1",
          [input(:base)]
        )

      execution = Journey.start_execution(original_graph)
      execution = Journey.set_value(execution, :base, "base_value")

      updated_graph =
        Journey.new_graph(
          "concurrent_test",
          "v1",
          [input(:base), input(:new1), input(:new2)]
        )

      # Perform migration multiple times rapidly
      # This should be safe due to advisory locks and hash checking
      results =
        for _i <- 1..5 do
          Task.async(fn ->
            Journey.Executions.migrate_to_current_graph_if_needed(execution)
          end)
        end
        |> Task.await_many(5000)

      # All results should have the same structure
      first_result = hd(results)

      for result <- results do
        assert result.id == first_result.id
        assert result.graph_hash == updated_graph.hash
        assert {:ok, "base_value"} == Journey.get_value(result, :base)
        assert {:error, :not_set} == Journey.get_value(result, :new1)
        assert {:error, :not_set} == Journey.get_value(result, :new2)
      end

      # Verify no duplicate value records were created by checking the database
      alias Journey.Persistence.Schema.Execution.Value

      value_counts =
        from(v in Value,
          where: v.execution_id == ^execution.id,
          group_by: v.node_name,
          select: {v.node_name, count(v.id)}
        )
        |> Journey.Repo.all()
        |> Enum.into(%{})

      # Each node should have exactly one value record
      expected_nodes = ["execution_id", "last_updated_at", "base", "new1", "new2"]

      for node_name <- expected_nodes do
        assert value_counts[node_name] == 1,
               "Node #{node_name} has #{value_counts[node_name]} value records, expected 1"
      end
    end

    test "migration with computation execution and new compute nodes" do
      # Create original graph with computation
      original_graph =
        Journey.new_graph(
          "computation_migration_test",
          "v1",
          [
            input(:i1),
            input(:i2),
            compute(:c, [:i1, :i2], fn %{i1: i1, i2: i2} -> {:ok, i1 + i2} end)
          ]
        )

      # Setup execution with computation
      execution = Journey.start_execution(original_graph)
      execution = Journey.set_value(execution, :i1, 10)
      execution = Journey.set_value(execution, :i2, 20)
      assert {:ok, 30} = Journey.get_value(execution, :c, wait_any: true)

      # Create updated graph (overwrites catalog)
      updated_graph =
        Journey.new_graph(
          "computation_migration_test",
          # Same name/version to overwrite
          "v1",
          [
            input(:i1),
            input(:i2),
            # New input
            input(:i3),
            compute(:c, [:i1, :i2], fn %{i1: i1, i2: i2} -> {:ok, i1 + i2} end),
            # New computation
            compute(:c2, [:i1, :i2, :i3], fn %{i1: i1, i2: i2, i3: i3} -> {:ok, i1 * i2 * i3} end)
          ]
        )

      # This set_value call will trigger migration automatically
      execution = Journey.set_value(execution, :i3, 5)

      # Verify original computation still works
      assert {:ok, 30} = Journey.get_value(execution, :c)

      # Verify new computation executes correctly
      # 10 * 20 * 5 = 1000
      assert {:ok, 1000} = Journey.get_value(execution, :c2, wait_any: true)

      # Verify migration occurred
      assert execution.graph_hash == updated_graph.hash
    end
  end
end
