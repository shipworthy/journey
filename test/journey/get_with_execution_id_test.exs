defmodule Journey.GetWithExecutionIdTest do
  use ExUnit.Case, async: true

  import Journey.Node

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  describe "Journey.get/3 with execution_id" do
    test "returns value and revision for set input node" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Alice")

      {:ok, value, revision} = Journey.get(execution.id, :user_name)
      assert value == "Alice"
      assert is_integer(revision)
      assert revision > 0
    end

    test "returns error for unset node with immediate wait" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert {:error, :not_set} = Journey.get(execution.id, :user_name)
    end

    test "returns error for non-existent node" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert {:error, :not_set} = Journey.get(execution.id, :non_existent_node)
    end

    test "waits for computed value with wait: :any" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Bob")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, value, revision} = Journey.get(execution.id, :reminder, wait: :any)
      assert value =~ "Bob"
      assert is_integer(revision)
      assert revision > 0

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "waits for newer revision with wait: {:newer_than, revision}" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Charlie")

      {:ok, _value, initial_revision} = Journey.get(execution.id, :user_name)

      # Update the value in background
      Task.async(fn ->
        Process.sleep(100)
        Journey.set(execution.id, :user_name, "Updated Charlie")
      end)

      {:ok, updated_value, new_revision} =
        Journey.get(execution.id, :user_name, wait: {:newer_than, initial_revision}, timeout: 1000)

      assert updated_value == "Updated Charlie"
      assert new_revision > initial_revision
    end

    test "rejects :newer wait option with execution_id" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, ~r/Invalid :wait option :newer for execution_id/, fn ->
        Journey.get(execution.id, :user_name, wait: :newer)
      end
    end

    test "supports timeout option" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Should timeout quickly when value is not available
      start_time = System.monotonic_time(:millisecond)
      result = Journey.get(execution.id, :user_name, wait: :any, timeout: 100)
      end_time = System.monotonic_time(:millisecond)

      assert result == {:error, :not_set}
      assert end_time - start_time >= 100
      # Should not take much longer than timeout
      assert end_time - start_time < 200
    end

    test "handles invalid wait options" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get(execution.id, :user_name, wait: :invalid_option)
      end
    end

    test "handles {:newer_than, revision} with non-integer revision" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get(execution.id, :user_name, wait: {:newer_than, "not_integer"})
      end
    end

    test "immediate return works correctly" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "David")

      # Should return immediately
      start_time = System.monotonic_time(:millisecond)
      {:ok, value, revision} = Journey.get(execution.id, :user_name, wait: :immediate)
      end_time = System.monotonic_time(:millisecond)

      assert value == "David"
      assert is_integer(revision)
      # Should be very fast
      assert end_time - start_time < 50
    end

    test "works with complex value types" do
      graph =
        Journey.new_graph(
          "complex value test",
          "v1.0.0",
          [
            input(:user_data)
          ]
        )

      execution = Journey.start_execution(graph)
      # Use string keys since atom keys get converted to strings in JSON storage
      complex_data = %{"name" => "Alice", "age" => 30, "tags" => ["admin", "user"]}
      execution = Journey.set(execution, :user_data, complex_data)

      {:ok, retrieved_data, revision} = Journey.get(execution.id, :user_data)
      assert retrieved_data == complex_data
      assert is_integer(revision)
    end
  end
end
