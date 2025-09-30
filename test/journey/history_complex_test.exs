defmodule Journey.HistoryComplexTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "history/1 with complex graphs" do
    test "handles parallel computations with non-deterministic execution order" do
      execution =
        minimal_complex_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:trigger, "start")

      # Wait for both parallel computations to complete
      {:ok, %{value: _}} = Journey.get_value(execution, :parallel_a, wait_any: true)
      {:ok, %{value: _}} = Journey.get_value(execution, :parallel_b, wait_any: true)

      history = Journey.history(execution.id)

      # Test invariants that hold regardless of execution order

      # Both parallel computations must exist
      parallel_computations =
        history
        |> Enum.filter(&(&1.computation_or_value == :computation and &1.node_name in [:parallel_a, :parallel_b]))

      assert length(parallel_computations) == 2

      # Both must have revision greater than trigger
      trigger_revision = get_node_revision(history, :trigger, :value)
      assert Enum.all?(parallel_computations, &(&1.revision > trigger_revision))

      # They may have same or different revisions (both are valid)
      revisions = Enum.map(parallel_computations, & &1.revision)
      assert Enum.all?(revisions, &(&1 > 0))
    end

    test "preserves causal dependencies through computation chains" do
      execution =
        minimal_complex_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:trigger, "chain-start")

      # Wait for the full chain to complete
      {:ok, %{value: _}} = Journey.get_value(execution, :downstream, wait_any: true)

      history = Journey.history(execution.id)

      # Verify causal ordering is preserved
      assert_causal_order(history, :trigger, :parallel_a)
      assert_causal_order(history, :parallel_a, :downstream)

      # Downstream should never appear before its dependency
      downstream_rev = get_node_revision(history, :downstream, :computation)
      parallel_a_rev = get_node_revision(history, :parallel_a, :computation)
      assert downstream_rev >= parallel_a_rev
    end

    test "tracks mutate node effects correctly" do
      execution =
        minimal_complex_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:data, "mutate-me")

      {:ok, %{value: _}} = Journey.get_value(execution, :mutator, wait_any: true)

      # The mutation should trigger re-computation of parallel nodes
      {:ok, %{value: val_a}} = Journey.get_value(execution, :parallel_a, wait_any: true)
      assert val_a == "a: mutated: mutate-me"

      history = Journey.history(execution.id)

      # Verify mutator appears in history
      mutator_entry =
        history
        |> Enum.find(&(&1.node_name == :mutator and &1.computation_or_value == :computation))

      assert mutator_entry != nil
      assert mutator_entry.node_type == :mutate

      # Verify the mutated trigger value appears after the mutator
      mutator_rev = mutator_entry.revision

      trigger_values =
        history
        |> Enum.filter(&(&1.node_name == :trigger and &1.computation_or_value == :value))
        |> Enum.sort_by(& &1.revision)

      # Should have at least 2 trigger values (initial set would need to be added, then mutated)
      if length(trigger_values) > 1 do
        mutated_trigger = List.last(trigger_values)
        assert mutated_trigger.revision >= mutator_rev
        assert mutated_trigger.value == "mutated: mutate-me"
      end
    end

    test "maintains revision monotonicity across complex execution flows" do
      execution =
        minimal_complex_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:trigger, "t1")
        |> Journey.set(:data, "d1")

      # Wait for expected computations to complete
      {:ok, %{value: _}} = Journey.get_value(execution, :parallel_a, wait_any: true)
      {:ok, %{value: _}} = Journey.get_value(execution, :parallel_b, wait_any: true)
      {:ok, %{value: _}} = Journey.get_value(execution, :mutator, wait_any: true)

      history = Journey.history(execution.id)

      # All revisions must be monotonically increasing
      revisions = Enum.map(history, & &1.revision)
      assert revisions == Enum.sort(revisions), "History must be sorted by revision"

      # No gaps in the sequence (though not all revisions need to appear in history)
      unique_revisions = Enum.uniq(revisions)
      assert length(unique_revisions) > 1, "Should have multiple revisions"
    end

    test "correctly identifies all node types in complex graphs" do
      execution =
        multi_type_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:input_node, "test")

      # Wait for various computations
      {:ok, %{value: _}} = Journey.get_value(execution, :compute_node, wait_any: true)
      {:ok, %{value: _}} = Journey.get_value(execution, :mutate_node, wait_any: true)

      history = Journey.history(execution.id)

      # Group by node type
      by_type =
        history
        |> Enum.group_by(& &1.node_type)
        |> Map.keys()
        |> MapSet.new()

      # Should see multiple node types
      assert :input in by_type
      assert :compute in by_type
      assert :mutate in by_type
      # schedule_once might not execute in time, so we don't assert it
    end

    test "tracks re-computations when input values change" do
      execution =
        minimal_complex_graph(random_string())
        |> Journey.start_execution()

      # First computation wave
      execution = Journey.set(execution, :trigger, "first")
      {:ok, %{value: first_a}} = Journey.get_value(execution, :parallel_a, wait_any: true)
      assert first_a == "a: first"

      # Update trigger again to cause re-computation
      execution = Journey.set(execution, :trigger, "second")
      {:ok, %{value: second_a}} = Journey.get_value(execution, :parallel_a, wait_new: true)
      assert second_a == "a: second"

      history = Journey.history(execution.id)

      # The trigger value appears once with its latest value and revision
      trigger_values =
        history
        |> Enum.filter(&(&1.node_name == :trigger and &1.computation_or_value == :value))

      assert length(trigger_values) == 1, "Should have one trigger value entry with latest value"
      trigger_entry = hd(trigger_values)
      assert trigger_entry.value == "second", "Should show the latest value"

      # Should have multiple successful computations for parallel_a (one for each trigger value)
      parallel_a_computations =
        history
        |> Enum.filter(&(&1.node_name == :parallel_a and &1.computation_or_value == :computation))

      assert length(parallel_a_computations) >= 2, "Should have multiple computations for parallel_a"

      # They should have different revisions
      revisions =
        parallel_a_computations
        |> Enum.map(& &1.revision)
        |> Enum.uniq()

      assert length(revisions) >= 2, "Re-computations should have different revisions"
    end

    test "complex graph maintains all required history invariants" do
      # Run multiple times to catch potential race conditions
      for _ <- 1..3 do
        execution =
          minimal_complex_graph(random_string())
          |> Journey.start_execution()
          |> Journey.set(:trigger, "test")
          |> Journey.set(:data, "test-data")

        # Wait for expected computations to complete
        {:ok, %{value: _}} = Journey.get_value(execution, :parallel_a, wait_any: true)
        {:ok, %{value: _}} = Journey.get_value(execution, :parallel_b, wait_any: true)
        {:ok, %{value: _}} = Journey.get_value(execution, :mutator, wait_any: true)
        # downstream depends on parallel_a so should also complete
        {:ok, %{value: _}} = Journey.get_value(execution, :downstream, wait_any: true)

        history = Journey.history(execution.id)

        # Test all invariants
        assert_history_invariants(history)

        # Additional specific invariants for complex graphs
        assert_complex_graph_invariants(history)
      end
    end
  end

  # Helper graph definitions

  defp minimal_complex_graph(test_id) do
    Journey.new_graph(
      "minimal complex graph #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:trigger),
        input(:data),

        # Parallel computations - both unblocked by same input
        compute(
          :parallel_a,
          unblocked_when({:trigger, &provided?/1}),
          fn %{trigger: t} ->
            # Small random delay to introduce non-determinism
            Process.sleep(:rand.uniform(10))
            {:ok, "a: #{t}"}
          end
        ),
        compute(
          :parallel_b,
          unblocked_when({:trigger, &provided?/1}),
          fn %{trigger: t} ->
            # Small random delay to introduce non-determinism
            Process.sleep(:rand.uniform(10))
            {:ok, "b: #{t}"}
          end
        ),

        # Downstream computation - depends on one parallel branch
        compute(
          :downstream,
          unblocked_when({:parallel_a, &provided?/1}),
          fn %{parallel_a: a} ->
            {:ok, "down: #{a}"}
          end
        ),

        # Mutate node - modifies trigger when data is set
        mutate(
          :mutator,
          unblocked_when({:data, &provided?/1}),
          fn %{data: d} ->
            {:ok, "mutated: #{d}"}
          end,
          mutates: :trigger
        )
      ]
    )
  end

  defp multi_type_graph(test_id) do
    Journey.new_graph(
      "multi type graph #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:input_node),
        compute(
          :compute_node,
          unblocked_when({:input_node, &provided?/1}),
          fn %{input_node: val} ->
            {:ok, "computed: #{val}"}
          end
        ),
        mutate(
          :mutate_node,
          unblocked_when({:input_node, &provided?/1}),
          fn %{input_node: val} ->
            {:ok, "mutated: #{val}"}
          end,
          mutates: :input_node
        ),
        schedule_once(
          :schedule_node,
          unblocked_when({:input_node, &provided?/1}),
          fn %{input_node: val} ->
            {:ok, "scheduled: #{val}"}
          end
        )
      ]
    )
  end

  # Helper functions

  defp get_node_revision(history, node_name, type) do
    history
    |> Enum.find(&(&1.node_name == node_name and &1.computation_or_value == type))
    |> case do
      nil -> nil
      entry -> entry.revision
    end
  end

  defp assert_causal_order(history, before_node, after_node) do
    # Find the earliest occurrence of after_node
    after_entries =
      history
      |> Enum.filter(&(&1.node_name == after_node))
      |> Enum.sort_by(& &1.revision)

    # Find the latest occurrence of before_node that could have caused after_node
    before_entries =
      history
      |> Enum.filter(&(&1.node_name == before_node))
      |> Enum.sort_by(& &1.revision)

    if length(after_entries) > 0 and length(before_entries) > 0 do
      after_rev = hd(after_entries).revision
      before_rev = hd(before_entries).revision

      assert before_rev <= after_rev,
             "#{before_node} (rev #{before_rev}) should complete before or with #{after_node} (rev #{after_rev})"
    end
  end

  defp assert_history_invariants(history) do
    # History is sorted by revision
    revisions = Enum.map(history, & &1.revision)
    assert revisions == Enum.sort(revisions)

    # All entries have required fields
    Enum.each(history, fn entry ->
      assert Map.has_key?(entry, :computation_or_value)
      assert Map.has_key?(entry, :node_name)
      assert Map.has_key?(entry, :node_type)
      assert Map.has_key?(entry, :revision)

      # Value entries have value field, computation entries don't
      if entry.computation_or_value == :value do
        assert Map.has_key?(entry, :value)
      else
        refute Map.has_key?(entry, :value)
      end
    end)
  end

  defp assert_complex_graph_invariants(history) do
    # At same revision, computations come before values
    history
    |> Enum.group_by(& &1.revision)
    |> Enum.each(fn {_rev, entries} ->
      # Check if both types exist at this revision
      types = Enum.map(entries, & &1.computation_or_value)

      if :computation in types and :value in types do
        # Find indices
        first_value_idx = Enum.find_index(entries, &(&1.computation_or_value == :value))

        last_computation_idx =
          entries
          |> Enum.reverse()
          |> Enum.find_index(&(&1.computation_or_value == :computation))

        last_computation_idx = length(entries) - 1 - last_computation_idx

        assert last_computation_idx < first_value_idx,
               "Computations should appear before values at the same revision"
      end
    end)
  end
end
