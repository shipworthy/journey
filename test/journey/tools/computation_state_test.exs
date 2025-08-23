defmodule Journey.Tools.ComputationStateTest do
  use ExUnit.Case, async: true

  import Journey.Node

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  describe "computation_state/2" do
    test "returns :not_set for computation that hasn't run yet" do
      graph =
        Journey.new_graph(
          "computation_state test graph not_set #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            compute(:greeting, [:user_name], fn %{user_name: name} ->
              {:ok, "Hello, #{name}"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)

      assert Journey.Tools.computation_state(execution.id, :greeting) == :not_set
    end

    test "returns :success for successful computation" do
      graph =
        Journey.new_graph(
          "computation_state test graph success #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            compute(:greeting, [:user_name], fn %{user_name: name} ->
              {:ok, "Hello, #{name}"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :user_name, "Alice")

      {:ok, _greeting} = Journey.get_value(execution, :greeting, wait_new: true)

      assert Journey.Tools.computation_state(execution.id, :greeting) == :success
    end

    test "returns :failed for failed computation" do
      graph =
        Journey.new_graph(
          "computation_state test graph failed #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            compute(:will_fail, [:value], fn _ ->
              {:error, "intentional failure"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :value, "test")

      {:error, _} = Journey.get_value(execution, :will_fail, wait_new: true)

      assert Journey.Tools.computation_state(execution.id, :will_fail) == :failed
    end

    test "returns :not_compute_node for input nodes" do
      graph =
        Journey.new_graph(
          "computation_state test graph input_node #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            compute(:greeting, [:user_name], fn %{user_name: name} ->
              {:ok, "Hello, #{name}"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)

      assert Journey.Tools.computation_state(execution.id, :user_name) == :not_compute_node
    end

    test "handles schedule_once nodes" do
      graph =
        Journey.new_graph(
          "computation_state test graph schedule_once #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            schedule_once(:scheduled_task, [:value], fn _ ->
              {:ok, System.system_time(:second) + 1}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :value, "trigger")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)
      {:ok, _} = Journey.get_value(execution, :scheduled_task, wait_new: true)

      assert Journey.Tools.computation_state(execution.id, :scheduled_task) == :success

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "handles schedule_recurring nodes" do
      graph =
        Journey.new_graph(
          "computation_state test graph schedule_recurring #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            schedule_recurring(:recurring_task, [:value], fn _ ->
              # Schedule to run 1 second in the future
              {:ok, System.system_time(:second) + 1}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :value, "trigger")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _schedule_time} = Journey.get_value(execution, :recurring_task, wait_new: true)
      Process.sleep(1000)

      reloaded = Journey.load(execution.id)
      computations = Journey.Executions.find_computations_by_node_name(reloaded, :recurring_task)

      assert length(computations) > 0
      assert Enum.any?(computations, fn c -> c.state == :success end)

      assert Journey.Tools.computation_state(execution.id, :recurring_task) in [:success, :not_set]

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "returns state of most recent computation after retries" do
      # Test that when a computation fails and is retried by the scheduler,
      # we get the state of the most recent attempt
      graph =
        Journey.new_graph(
          "computation_state test graph retries #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            compute(
              :always_fails,
              [:value],
              fn %{value: v} ->
                # This computation always fails, triggering retries
                {:error, "intentional failure for #{v}"}
              end,
              # Will attempt 3 times total (initial + 2 retries)
              max_retries: 2
            )
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :value, "test")

      # Start background sweeps to handle retries
      # In test environment, sweeps run every 500ms
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Wait for initial computation to fail
      {:error, _} = Journey.get_value(execution, :always_fails, wait_new: true)

      # Wait for retries to be scheduled and executed
      # Each sweep (every 500ms) will pick up and execute retry computations
      # With max_retries: 2, we expect 3 total attempts (initial + 2 retries)
      # Allow time for 2 retries to be processed
      Process.sleep(2000)

      # Load the execution to get all computation attempts
      reloaded = Journey.load(execution.id)

      # Verify we have multiple failed computation attempts for this node
      computations = Journey.Executions.find_computations_by_node_name(reloaded, :always_fails)

      # Should have 3 computation records (initial + 2 retries)
      # Note: max_retries: 2 means 2 retries AFTER the initial attempt
      # At least initial + 1 retry
      assert length(computations) >= 2

      # All should be failed
      assert Enum.all?(computations, fn c -> c.state == :failed end)

      # computation_state should return the state of the most recent attempt
      assert Journey.Tools.computation_state(execution.id, :always_fails) == :failed

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "raises for non-existent execution" do
      assert_raise KeyError, fn ->
        Journey.Tools.computation_state("EXEC_DOESNT_EXIST", :some_node)
      end
    end

    test "handles mutate nodes" do
      graph =
        Journey.new_graph(
          "computation_state test graph mutate #{__MODULE__}",
          "1.0.0",
          [
            input(:original),
            mutate(
              :modifier,
              [:original],
              fn %{original: val} ->
                {:ok, "modified: #{val}"}
              end,
              mutates: :original
            )
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :original, "test")

      {:ok, _} = Journey.get_value(execution, :modifier, wait_new: true)

      assert Journey.Tools.computation_state(execution.id, :modifier) == :success
    end
  end
end
