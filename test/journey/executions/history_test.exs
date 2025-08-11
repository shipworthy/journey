defmodule Journey.Executions.HistoryTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "history/1" do
    test "returns initial values for new execution" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()

      history = Journey.Executions.history(execution.id)

      # New execution should have execution_id and last_updated_at set initially
      assert length(history) == 2

      # All initial values should be at revision 0
      assert Enum.all?(history, &(&1.ex_revision_at_completion == 0))

      # All should be values (not computations)
      assert Enum.all?(history, &(&1.computation_or_value == :value))

      # Verify node names
      node_names = Enum.map(history, & &1.node_name)
      assert :execution_id in node_names
      assert :last_updated_at in node_names

      # All ex_revision_at_start should be nil
      assert Enum.all?(history, &(&1.ex_revision_at_start == nil))
    end

    test "tracks value changes in history" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()

      # Set a value
      execution = Journey.set_value(execution, :input_a, "test_value")

      history = Journey.Executions.history(execution.id)

      # Should have initial 2 + the new value set
      assert length(history) >= 3

      # Find the input_a entry
      input_a_entry = Enum.find(history, &(&1.node_name == :input_a))
      assert input_a_entry != nil
      assert input_a_entry.computation_or_value == :value
      assert input_a_entry.node_type == :input
      assert input_a_entry.value == "test_value"
      assert input_a_entry.ex_revision_at_completion > 0
      assert input_a_entry.ex_revision_at_start == nil
    end

    test "tracks successful computations in history" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:input_a, "test_input")

      # Wait for computation to complete
      {:ok, computed_value} = Journey.get_value(execution, :computed_a, wait_any: true)
      assert computed_value == "computed: test_input"

      history = Journey.Executions.history(execution.id)

      # Find the computation entry
      computation_entry = Enum.find(history, &(&1.node_name == :computed_a))
      assert computation_entry != nil
      assert computation_entry.computation_or_value == :computation
      assert computation_entry.node_type == :compute
      assert computation_entry.ex_revision_at_completion > 0
      assert computation_entry.ex_revision_at_start == nil
      # Computation entries don't have a value field
      refute Map.has_key?(computation_entry, :value)
    end

    test "returns history in correct chronological order" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()

      # Perform multiple operations
      execution = Journey.set_value(execution, :input_a, "first")
      {:ok, _} = Journey.get_value(execution, :computed_a, wait_any: true)

      execution = Journey.set_value(execution, :input_a, "second")
      {:ok, _} = Journey.get_value(execution, :computed_a, wait_new: true)

      history = Journey.Executions.history(execution.id)

      # Verify ordering by revision
      revisions = Enum.map(history, & &1.ex_revision_at_completion)
      assert revisions == Enum.sort(revisions), "History should be sorted by revision"

      # Verify we have multiple revisions
      unique_revisions = Enum.uniq(revisions)
      assert length(unique_revisions) > 1, "Should have multiple revisions in history"
    end

    test "computations appear before values at same revision" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:input_a, "test")

      # Wait for computation
      {:ok, _} = Journey.get_value(execution, :computed_a, wait_any: true)

      history = Journey.Executions.history(execution.id)

      # Group by revision
      by_revision =
        history
        |> Enum.group_by(& &1.ex_revision_at_completion)

      # For any revision that has both computations and values,
      # computations should come first
      Enum.each(by_revision, fn {_revision, entries} ->
        types = Enum.map(entries, & &1.computation_or_value)

        if :computation in types and :value in types do
          # Find first computation and last value indices
          first_computation = Enum.find_index(types, &(&1 == :computation))
          last_value = Enum.find_index(Enum.reverse(types), &(&1 == :value))
          last_value = length(types) - 1 - last_value

          assert first_computation < last_value,
                 "Computations should appear before values at the same revision"
        end
      end)
    end

    test "only includes values with set_time" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()

      # The graph has input_b defined but never set
      history = Journey.Executions.history(execution.id)

      # input_b should not appear in history since it was never set
      input_b_entry = Enum.find(history, &(&1.node_name == :input_b))
      assert input_b_entry == nil, "Unset values should not appear in history"

      # But execution_id and last_updated_at should be there (set initially)
      assert Enum.any?(history, &(&1.node_name == :execution_id))
      assert Enum.any?(history, &(&1.node_name == :last_updated_at))
    end

    test "only includes successful computations" do
      # First, create execution that will succeed
      execution =
        failing_history_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:will_fail, false)

      # Wait for successful computation
      {:ok, _} = Journey.get_value(execution, :maybe_fails, wait_any: true)

      # Now set it to fail to create a failed computation
      execution = Journey.set_value(execution, :will_fail, true)

      # Wait for the computation to attempt and fail
      Process.sleep(200)

      history = Journey.Executions.history(execution.id)

      # Count how many computation entries for maybe_fails
      maybe_fails_computations =
        history
        |> Enum.filter(&(&1.node_name == :maybe_fails and &1.computation_or_value == :computation))

      # Should only have the successful one, not the failed attempt
      assert length(maybe_fails_computations) == 1

      # The successful computation should be in the history
      successful_computation = hd(maybe_fails_computations)
      assert successful_computation.computation_or_value == :computation
      assert successful_computation.node_type == :compute
    end
  end

  # Helper graph with simple structure
  defp simple_history_graph(test_id) do
    Journey.new_graph(
      "simple history test #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:input_a),
        # Intentionally never set, to test filtering
        input(:input_b),
        compute(
          :computed_a,
          unblocked_when({:input_a, &provided?/1}),
          fn %{input_a: a} ->
            {:ok, "computed: #{a}"}
          end
        )
      ]
    )
  end

  # Helper graph with failing computation
  defp failing_history_graph(test_id) do
    Journey.new_graph(
      "failing history test #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:will_fail),
        compute(
          :maybe_fails,
          unblocked_when({:will_fail, &provided?/1}),
          fn %{will_fail: should_fail} ->
            if should_fail do
              {:error, "intentional failure"}
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
