defmodule Journey.HistorySimpleTest do
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

      history = Journey.history(execution.id)

      # New execution should have execution_id and last_updated_at set initially
      assert length(history) == 2

      # All initial values should be at revision 0
      assert Enum.all?(history, &(&1.revision == 0))

      # All should be values (not computations)
      assert Enum.all?(history, &(&1.computation_or_value == :value))

      # Verify node names
      node_names = Enum.map(history, & &1.node_name)
      assert :execution_id in node_names
      assert :last_updated_at in node_names

      # All entries should have a revision
      assert Enum.all?(history, &Map.has_key?(&1, :revision))
    end

    test "tracks value changes in history" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()

      # Set a value
      execution = Journey.set_value(execution, :input_a, "test_value")

      history = Journey.history(execution.id)

      # Should have initial 2 + the new value set
      assert length(history) >= 3

      # Find the input_a entry
      input_a_entry = Enum.find(history, &(&1.node_name == :input_a))
      assert input_a_entry != nil
      assert input_a_entry.computation_or_value == :value
      assert input_a_entry.node_type == :input
      assert input_a_entry.value == "test_value"
      assert input_a_entry.revision == 1
    end

    test "tracks successful computations in history" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:input_a, "test_input")

      # Wait for computation to complete
      {:ok, computed_value} = Journey.get_value(execution, :computed_a, wait_any: true)
      assert computed_value == "computed: test_input"

      history = Journey.history(execution.id)

      # Find the computation entry
      computation_entry = Enum.find(history, &(&1.node_name == :computed_a))
      assert computation_entry != nil
      assert computation_entry.computation_or_value == :computation
      assert computation_entry.node_type == :compute
      assert computation_entry.revision == 3
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

      history = Journey.history(execution.id)

      # Redact dynamic timestamps in last_updated_at entries
      redacted_history =
        Enum.map(history, fn entry ->
          case entry.node_name do
            :last_updated_at -> %{entry | value: 1_234_567_890}
            _ -> entry
          end
        end)

      # Expected history based on actual behavior:
      # History only shows current state of nodes, not all intermediate states
      # Only successful computations and final values appear
      expected_history = [
        # Revision 0: Initial execution state
        %{node_name: :execution_id, node_type: :input, computation_or_value: :value, value: execution.id, revision: 0},

        # Revision 3: First successful computation (for "first" input)
        %{node_name: :computed_a, node_type: :compute, computation_or_value: :computation, revision: 3},

        # Revision 4: Set input_a to "second" (final value)
        %{node_name: :input_a, node_type: :input, computation_or_value: :value, value: "second", revision: 4},

        # Revision 6: Second computation and its result (for "second" input)
        %{node_name: :computed_a, node_type: :compute, computation_or_value: :computation, revision: 6},
        %{
          node_name: :computed_a,
          node_type: :compute,
          computation_or_value: :value,
          value: "computed: second",
          revision: 6
        },
        %{
          node_name: :last_updated_at,
          node_type: :input,
          computation_or_value: :value,
          value: 1_234_567_890,
          revision: 6
        }
      ]

      assert redacted_history == expected_history
    end

    test "computations appear before values at same revision" do
      execution =
        simple_history_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:input_a, "test")

      # Wait for computation
      {:ok, _} = Journey.get_value(execution, :computed_a, wait_any: true)

      history = Journey.history(execution.id)

      # Group by revision
      by_revision =
        history
        |> Enum.group_by(& &1.revision)

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
      history = Journey.history(execution.id)

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

      # Wait for the computation to re-run and fail permanently (after max_retries: 1)
      {:error, :computation_failed} = Journey.get_value(execution, :maybe_fails, wait_new: true)

      history = Journey.history(execution.id)

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
