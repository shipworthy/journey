defmodule Journey.GetTest do
  use ExUnit.Case, async: true

  import Journey.Node

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  describe "Journey.get/3" do
    test "returns value and revision for set input node" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Alice")

      {:ok, value, revision} = Journey.get(execution, :user_name)
      assert value == "Alice"
      assert is_integer(revision)
      assert revision > 0
    end

    test "returns error for unset node with immediate wait" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert {:error, :not_set} = Journey.get(execution, :user_name)
    end

    test "waits for computed value with wait: :any" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Bob")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, value, revision} = Journey.get(execution, :reminder, wait: :any)
      assert value =~ "Bob"
      assert is_integer(revision)
      assert revision > 0

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "waits for newer revision with wait: {:newer_than, revision}" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Carol")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Get initial value and revision
      {:ok, initial_value, initial_revision} = Journey.get(execution, :reminder, wait: :any)
      assert initial_value =~ "Carol"
      assert is_integer(initial_revision)

      # Trigger recomputation
      execution = Journey.set(execution, :user_name, "Dave")

      # Wait for newer revision
      {:ok, new_value, new_revision} = Journey.get(execution, :reminder, wait: {:newer_than, initial_revision})
      assert new_value =~ "Dave"
      assert new_revision > initial_revision

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "waits for newer revision with wait: :newer" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Eve")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Get initial value
      {:ok, _, initial_revision} = Journey.get(execution, :reminder, wait: :any)

      # Update the input to trigger recomputation
      execution = Journey.set(execution, :user_name, "Frank")

      # Wait for newer revision
      {:ok, new_value, new_revision} = Journey.get(execution, :reminder, wait: :newer)
      assert new_value =~ "Frank"
      assert new_revision > initial_revision

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "respects timeout option" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Should timeout quickly when waiting for unset value
      assert {:error, :not_set} = Journey.get(execution, :user_name, wait: :any, timeout: 100)
    end

    test "works with infinity timeout" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Grace")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, value, revision} = Journey.get(execution, :reminder, wait: :any, timeout: :infinity)
      assert value =~ "Grace"
      assert is_integer(revision)

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "returns error for computation failure" do
      graph = create_failing_graph()
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :will_fail, true)

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      assert {:error, :computation_failed} = Journey.get(execution, :result, wait: :any)

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "raises error for unknown node" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise RuntimeError, fn ->
        Journey.get(execution, :unknown_node)
      end
    end

    test "revision matches value atomically - no race condition" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "Henry")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Get value and revision atomically
      {:ok, value, revision} = Journey.get(execution, :reminder, wait: :any)

      # Reload execution and verify the revision matches the value we got
      reloaded = Journey.load(execution.id)
      reminder_value_record = Enum.find(reloaded.values, fn v -> v.node_name == :reminder end)

      assert reminder_value_record.node_value == value
      assert reminder_value_record.ex_revision == revision

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "validates option keys" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, fn ->
        Journey.get(execution, :user_name, invalid_option: true)
      end
    end

    test "validates wait option values" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, fn ->
        Journey.get(execution, :user_name, wait: :invalid_wait_option)
      end
    end
  end

  describe "Journey.get/3 - comprehensive value types" do
    test "handles different value types with correct revisions" do
      graph = create_multi_type_graph()
      execution = Journey.start_execution(graph)

      # Test string value
      execution = Journey.set(execution, :string_val, "hello world")
      {:ok, "hello world", rev1} = Journey.get(execution, :string_val)
      assert is_integer(rev1) and rev1 > 0

      # Test number value
      execution = Journey.set(execution, :number_val, 42)
      {:ok, 42, rev2} = Journey.get(execution, :number_val)
      assert rev2 > rev1

      # Test boolean value
      execution = Journey.set(execution, :bool_val, true)
      {:ok, true, rev3} = Journey.get(execution, :bool_val)
      assert rev3 > rev2

      # Test map value (note: atom keys get converted to strings)
      map_val = %{"key" => "value", "count" => 5}
      execution = Journey.set(execution, :map_val, map_val)
      {:ok, returned_map, rev4} = Journey.get(execution, :map_val)
      assert returned_map == map_val
      assert rev4 > rev3

      # Test list value (note: maps in lists also get atom keys converted)
      list_val = [1, 2, "three", %{"four" => 4}]
      execution = Journey.set(execution, :list_val, list_val)
      {:ok, returned_list, rev5} = Journey.get(execution, :list_val)
      assert returned_list == list_val
      assert rev5 > rev4

      # Test nil value
      execution = Journey.set(execution, :nil_val, nil)
      {:ok, nil, rev6} = Journey.get(execution, :nil_val)
      assert rev6 > rev5
    end
  end

  describe "Journey.get/3 - edge cases and error conditions" do
    test "validates timeout option values" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Invalid timeout - string
      assert_raise ArgumentError, ~r/Invalid timeout value/, fn ->
        Journey.get(execution, :user_name, wait: :any, timeout: "invalid")
      end

      # Invalid timeout - negative number
      assert_raise ArgumentError, ~r/Invalid timeout value/, fn ->
        Journey.get(execution, :user_name, wait: :any, timeout: -100)
      end

      # Invalid timeout - zero
      assert_raise ArgumentError, ~r/Invalid timeout value/, fn ->
        Journey.get(execution, :user_name, wait: :any, timeout: 0)
      end
    end

    test "validates wait option with newer_than requires integer" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get(execution, :user_name, wait: {:newer_than, "not_integer"})
      end

      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get(execution, :user_name, wait: {:newer_than, 1.5})
      end
    end

    test "validates wait option combinations" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Invalid wait option types
      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get(execution, :user_name, wait: "invalid")
      end

      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get(execution, :user_name, wait: [:invalid, :list])
      end
    end

    test "raises error for non-existent node with detailed message" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      assert_raise RuntimeError, ~r/'.*unknown_node.*' is not a known node/, fn ->
        Journey.get(execution, :unknown_node)
      end
    end
  end

  describe "Journey.get/3 - wait modes comprehensive" do
    test "wait: :immediate returns immediately for all states" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Unset node - immediate return
      assert {:error, :not_set} = Journey.get(execution, :user_name, wait: :immediate)

      # Set node - immediate return
      execution = Journey.set(execution, :user_name, "Alice")
      {:ok, "Alice", _} = Journey.get(execution, :user_name, wait: :immediate)
    end

    test "wait: :any with different timeout values" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Short timeout for unset value
      assert {:error, :not_set} = Journey.get(execution, :user_name, wait: :any, timeout: 50)

      # Longer timeout with value set in background
      Task.async(fn ->
        Process.sleep(100)
        Journey.set(execution, :user_name, "Bob")
      end)

      {:ok, "Bob", _} = Journey.get(execution, :user_name, wait: :any, timeout: 500)
    end

    test "wait: :newer works correctly for immediate updates" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :user_name, "Alice")

      # Update value in background
      Task.async(fn ->
        Process.sleep(50)
        Journey.set(execution, :user_name, "Bob")
      end)

      {:ok, "Bob", _} = Journey.get(execution, :user_name, wait: :newer, timeout: 300)
    end

    test "wait: {:newer_than, revision} with specific revision numbers" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      execution = Journey.set(execution, :user_name, "Alice")
      {:ok, _, rev1} = Journey.get(execution, :user_name)

      execution = Journey.set(execution, :user_name, "Bob")
      {:ok, _, _rev2} = Journey.get(execution, :user_name)

      execution = Journey.set(execution, :user_name, "Carol")

      # Should get Carol since revision will be > rev1
      {:ok, "Carol", rev3} = Journey.get(execution, :user_name, wait: {:newer_than, rev1})
      assert rev3 > rev1

      # Should timeout since no revision > rev3 exists
      assert {:error, :not_set} = Journey.get(execution, :user_name, wait: {:newer_than, rev3}, timeout: 100)
    end
  end

  describe "Journey.get/3 - revision atomicity and consistency" do
    test "multiple gets return consistent revision for same value" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :user_name, "Alice")

      {:ok, "Alice", rev1} = Journey.get(execution, :user_name)
      {:ok, "Alice", rev2} = Journey.get(execution, :user_name)
      {:ok, "Alice", rev3} = Journey.get(execution, :user_name)

      assert rev1 == rev2
      assert rev2 == rev3
    end

    test "revision increments correctly with value changes" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      execution = Journey.set(execution, :user_name, "Alice")
      {:ok, "Alice", rev1} = Journey.get(execution, :user_name)

      execution = Journey.set(execution, :user_name, "Bob")
      {:ok, "Bob", rev2} = Journey.get(execution, :user_name)

      execution = Journey.set(execution, :user_name, "Carol")
      {:ok, "Carol", rev3} = Journey.get(execution, :user_name)

      assert rev2 > rev1
      assert rev3 > rev2
    end

    test "computed values have correct revisions" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :user_name, "Alice")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, greeting_value, greeting_rev} = Journey.get(execution, :greeting, wait: :any)
      {:ok, _user_value, user_rev} = Journey.get(execution, :user_name)

      assert greeting_value == "Hello, Alice"
      assert is_integer(greeting_rev)
      assert greeting_rev >= user_rev

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end

  describe "Journey.get/3 - failed computations" do
    test "returns computation_failed for permanently failed nodes" do
      graph = create_failing_graph()
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :will_fail, true)

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Should return computation_failed after retries are exhausted
      assert {:error, :computation_failed} = Journey.get(execution, :result, wait: :any)

      # Subsequent calls should return immediately
      assert {:error, :computation_failed} = Journey.get(execution, :result, wait: :immediate)
      assert {:error, :computation_failed} = Journey.get(execution, :result)

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "failed computation returns computation_failed consistently" do
      graph = create_failing_graph()
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :will_fail, true)

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Wait for the computation to fail
      assert {:error, :computation_failed} = Journey.get(execution, :result, wait: :any)

      # Subsequent calls should consistently return the same error
      assert {:error, :computation_failed} = Journey.get(execution, :result, wait: :immediate)
      assert {:error, :computation_failed} = Journey.get(execution, :result)

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end

  describe "Journey.get/3 - execution vs execution_id integration" do
    test "execution_id variant works the same as execution variant" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph) |> Journey.set(:user_name, "Alice")

      # Both should return the same result
      {:ok, value1, revision1} = Journey.get(execution, :user_name)
      {:ok, value2, revision2} = Journey.get(execution.id, :user_name)

      assert value1 == value2
      assert revision1 == revision2
    end

    test "execution_id variant with wait options" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Test that execution_id variant supports the allowed wait options
      assert {:error, :not_set} = Journey.get(execution.id, :user_name, wait: :immediate)

      # Set value in background and test wait: :any
      Task.async(fn ->
        Process.sleep(100)
        Journey.set(execution.id, :user_name, "Bob")
      end)

      {:ok, "Bob", revision} = Journey.get(execution.id, :user_name, wait: :any, timeout: 1000)
      assert is_integer(revision)
    end

    test "execution_id variant rejects :newer wait option" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      # Should raise error because :newer needs current execution context
      assert_raise ArgumentError, ~r/Invalid :wait option :newer for execution_id/, fn ->
        Journey.get(execution.id, :user_name, wait: :newer)
      end
    end

    test "execution_id variant supports {:newer_than, revision}" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph) |> Journey.set(:user_name, "Alice")

      {:ok, _value, initial_revision} = Journey.get(execution.id, :user_name)

      # Update value
      execution = Journey.set(execution, :user_name, "Updated Alice")

      # Both variants should work with {:newer_than, revision}
      {:ok, updated_value1, new_revision1} = Journey.get(execution, :user_name, wait: {:newer_than, initial_revision})

      {:ok, updated_value2, new_revision2} =
        Journey.get(execution.id, :user_name, wait: {:newer_than, initial_revision})

      assert updated_value1 == "Updated Alice"
      assert updated_value2 == "Updated Alice"
      assert new_revision1 == new_revision2
      assert new_revision1 > initial_revision
    end

    test "both variants handle complex data types identically" do
      graph =
        Journey.new_graph(
          "complex data test",
          "v1.0.0",
          [input(:complex_data)]
        )

      execution = Journey.start_execution(graph)
      # Use string keys since atom keys get converted to strings in JSON storage
      complex_data = %{"name" => "Test", "list" => [1, 2, 3], "nested" => %{"key" => "value"}}
      execution = Journey.set(execution, :complex_data, complex_data)

      {:ok, data1, rev1} = Journey.get(execution, :complex_data)
      {:ok, data2, rev2} = Journey.get(execution.id, :complex_data)

      assert data1 == data2
      assert data1 == complex_data
      assert rev1 == rev2
    end

    test "performance comparison - execution_id should be faster for simple reads" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph) |> Journey.set(:user_name, "Performance Test")

      # Warm up
      Journey.get(execution, :user_name)
      Journey.get(execution.id, :user_name)

      # Time execution struct variant
      start_time = System.monotonic_time(:microsecond)

      for _ <- 1..100 do
        {:ok, _, _} = Journey.get(execution, :user_name)
      end

      execution_time = System.monotonic_time(:microsecond) - start_time

      # Time execution_id variant
      start_time = System.monotonic_time(:microsecond)

      for _ <- 1..100 do
        {:ok, _, _} = Journey.get(execution.id, :user_name)
      end

      execution_id_time = System.monotonic_time(:microsecond) - start_time

      # execution_id variant should be at least as fast, often faster
      # Allow some variance in testing environment
      assert execution_id_time <= execution_time * 1.2

      # Both should complete in reasonable time (less than 100ms for 100 calls)
      assert execution_time < 100_000
      assert execution_id_time < 100_000
    end
  end

  # Helper functions
  defp create_multi_type_graph do
    Journey.new_graph(
      "multi type test graph #{__MODULE__}",
      "1.0.0",
      [
        input(:string_val),
        input(:number_val),
        input(:bool_val),
        input(:map_val),
        input(:list_val),
        input(:nil_val)
      ]
    )
  end

  defp create_failing_graph do
    Journey.new_graph(
      "failing computation test graph #{__MODULE__}",
      "1.0.0",
      [
        input(:will_fail),
        compute(
          :result,
          [:will_fail],
          fn %{will_fail: should_fail} ->
            if should_fail do
              {:error, "intentional test failure"}
            else
              {:ok, "success"}
            end
          end,
          max_retries: 1
        )
      ]
    )
  end
end
