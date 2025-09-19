defmodule Journey.JourneyUnsetValueTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "unset |" do
    test "unsetting a previously set value" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Mario", _} = Journey.get(execution, :first_name)
      {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)

      execution_after_set = Journey.load(execution)
      original_revision = execution_after_set.revision

      # Unset the value
      execution_after_unset = Journey.unset(execution, :first_name)

      # Value should be not_set after unset
      assert Journey.get(execution_after_unset, :first_name) == {:error, :not_set}

      # Revision should be incremented
      assert execution_after_unset.revision > original_revision
    end

    test "unsetting a value that was never set" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      # Value starts as not_set
      assert Journey.get(execution, :first_name) == {:error, :not_set}

      original_revision = execution.revision

      # Unset a value that was never set
      execution_after_unset = Journey.unset(execution, :first_name)

      # Value should still be not_set
      assert Journey.get(execution_after_unset, :first_name) == {:error, :not_set}

      # Revision should NOT change since value was already unset
      assert execution_after_unset.revision == original_revision
    end

    test "unsetting a value multiple times" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Mario", _} = Journey.get(execution, :first_name)

      # First unset
      execution_after_first_unset = Journey.unset(execution, :first_name)
      assert Journey.get(execution_after_first_unset, :first_name) == {:error, :not_set}
      first_unset_revision = execution_after_first_unset.revision

      # Second unset (should be idempotent)
      execution_after_second_unset = Journey.unset(execution_after_first_unset, :first_name)
      assert Journey.get(execution_after_second_unset, :first_name) == {:error, :not_set}

      # Revision should not change on second unset
      assert execution_after_second_unset.revision == first_unset_revision
    end

    test "unset then set again" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Mario", _} = Journey.get(execution, :first_name)
      {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)

      # Unset the value
      execution_after_unset = Journey.unset(execution, :first_name)
      assert Journey.get(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :greeting) == {:error, :not_set}

      # Set a new value
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Luigi")
      {:ok, "Luigi", _} = Journey.get(execution_after_reset, :first_name)
      {:ok, "Hello, Luigi", _} = Journey.get(execution_after_reset, :greeting, wait: :any)
    end

    test "unsetting value increments revision" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Mario", _} = Journey.get(execution, :first_name)
      {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)

      original_revision = execution.revision

      # Unset the value - this should increment revision
      execution_after_unset = Journey.unset(execution, :first_name)

      # Revision should be incremented
      assert execution_after_unset.revision > original_revision
      assert Journey.get(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :greeting) == {:error, :not_set}

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Bob")
      assert execution_after_reset.revision > execution_after_unset.revision
      {:ok, "Bob", _} = Journey.get(execution_after_reset, :first_name)
      {:ok, "Hello, Bob", _} = Journey.get(execution_after_reset, :greeting, wait: :any)
    end

    test "unknown node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':last_name' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.unset(execution, :last_name)
                   end
    end

    test "not an input node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':greeting' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.unset(execution, :greeting)
                   end
    end

    test "unset with execution_id uses hardcoded revision assertion" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)
      execution_after_unset = Journey.unset(execution.id, :first_name)
      # Revision will be higher due to cascading invalidation
      assert execution_after_unset.revision > 4
      assert Journey.get(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :greeting) == {:error, :not_set}

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Peach")
      {:ok, "Peach", _} = Journey.get(execution_after_reset, :first_name)
      {:ok, "Hello, Peach", _} = Journey.get(execution_after_reset, :greeting, wait: :any)
    end

    test "unset keeps last_updated_at set (not unset)" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)

      values_before = Journey.values_all(execution)
      assert {:set, _timestamp} = values_before.last_updated_at

      execution_after_unset = Journey.unset(execution, :first_name)
      values_after = Journey.values_all(execution_after_unset)

      assert {:set, _new_timestamp} = values_after.last_updated_at
      assert values_after.first_name == :not_set
      assert values_after.greeting == :not_set

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Toad")
      {:ok, "Toad", _} = Journey.get(execution_after_reset, :first_name)
      {:ok, "Hello, Toad", _} = Journey.get(execution_after_reset, :greeting, wait: :any)

      # Verify last_updated_at is still set and was updated
      values_after_reset = Journey.values_all(execution_after_reset)
      assert {:set, _final_timestamp} = values_after_reset.last_updated_at
    end

    test "unset updates last_updated_at timestamp" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)

      values_before = Journey.values_all(execution)
      {:set, initial_timestamp} = values_before.last_updated_at

      Process.sleep(1000)

      execution_after_unset = Journey.unset(execution.id, :first_name)
      values_after = Journey.values_all(execution_after_unset)

      {:set, new_timestamp} = values_after.last_updated_at
      assert new_timestamp > initial_timestamp

      # Sleep again before re-setting
      Process.sleep(1000)

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Yoshi")
      {:ok, "Yoshi", _} = Journey.get(execution_after_reset, :first_name)
      {:ok, "Hello, Yoshi", _} = Journey.get(execution_after_reset, :greeting, wait: :any)

      # Verify last_updated_at was updated again
      values_after_reset = Journey.values_all(execution_after_reset)
      {:set, reset_timestamp} = values_after_reset.last_updated_at
      assert reset_timestamp > new_timestamp
    end
  end

  describe "cascade unset |" do
    test "cascades through multi-level dependencies" do
      # A → B → C chain
      graph =
        Journey.new_graph(
          "cascade graph #{__MODULE__} #{random_string()}",
          "1.0.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, "B:#{a}"} end),
            compute(:c, [:b], fn %{b: b} -> {:ok, "C:#{b}"} end)
          ]
        )

      execution =
        graph
        |> Journey.start_execution()
        |> Journey.set(:a, "value")

      # Wait for all computations
      {:ok, "B:value", _} = Journey.get(execution, :b, wait: :any)
      {:ok, "C:B:value", _} = Journey.get(execution, :c, wait: :any)

      # Unset :a should cascade to :b and :c
      execution_after_unset = Journey.unset(execution, :a)

      assert Journey.get(execution_after_unset, :a) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :b) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :c) == {:error, :not_set}

      # Setting :a again should trigger recomputation
      execution_reset = Journey.set(execution_after_unset, :a, "new")
      {:ok, "B:new", _} = Journey.get(execution_reset, :b, wait: :any)
      {:ok, "C:B:new", _} = Journey.get(execution_reset, :c, wait: :any)
    end

    test "handles diamond dependencies" do
      # A → B,C → D diamond pattern
      graph =
        Journey.new_graph(
          "diamond graph #{__MODULE__} #{random_string()}",
          "1.0.0",
          [
            input(:a),
            compute(:b, [:a], fn %{a: a} -> {:ok, "B:#{a}"} end),
            compute(:c, [:a], fn %{a: a} -> {:ok, "C:#{a}"} end),
            compute(:d, [:b, :c], fn %{b: b, c: c} -> {:ok, "D:#{b}+#{c}"} end)
          ]
        )

      execution =
        graph
        |> Journey.start_execution()
        |> Journey.set(:a, "val")

      # Wait for all computations
      {:ok, "B:val", _} = Journey.get(execution, :b, wait: :any)
      {:ok, "C:val", _} = Journey.get(execution, :c, wait: :any)
      {:ok, "D:B:val+C:val", _} = Journey.get(execution, :d, wait: :any)

      # Unset :a should cascade to :b, :c, and :d
      execution_after_unset = Journey.unset(execution, :a)

      assert Journey.get(execution_after_unset, :a) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :b) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :c) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :d) == {:error, :not_set}

      # Set :a again and verify all computations trigger
      execution_reset = Journey.set(execution_after_unset, :a, "reset")
      {:ok, "reset", _} = Journey.get(execution_reset, :a)
      {:ok, "B:reset", _} = Journey.get(execution_reset, :b, wait: :any)
      {:ok, "C:reset", _} = Journey.get(execution_reset, :c, wait: :any)
      {:ok, "D:B:reset+C:reset", _} = Journey.get(execution_reset, :d, wait: :any)
    end

    test "partial dependencies - only affected nodes cascade" do
      # Two independent inputs
      graph =
        Journey.new_graph(
          "partial deps graph #{__MODULE__} #{random_string()}",
          "1.0.0",
          [
            input(:x),
            input(:y),
            compute(:from_x, [:x], fn %{x: x} -> {:ok, "X:#{x}"} end),
            compute(:from_y, [:y], fn %{y: y} -> {:ok, "Y:#{y}"} end),
            compute(:from_both, [:x, :y], fn %{x: x, y: y} -> {:ok, "Both:#{x}+#{y}"} end)
          ]
        )

      execution =
        graph
        |> Journey.start_execution()
        |> Journey.set(:x, "xval")
        |> Journey.set(:y, "yval")

      # Wait for computations
      {:ok, "X:xval", _} = Journey.get(execution, :from_x, wait: :any)
      {:ok, "Y:yval", _} = Journey.get(execution, :from_y, wait: :any)
      {:ok, "Both:xval+yval", _} = Journey.get(execution, :from_both, wait: :any)

      # Unset :x should cascade to :from_x and :from_both, but NOT :from_y
      execution_after_unset = Journey.unset(execution, :x)

      assert Journey.get(execution_after_unset, :x) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :from_x) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :from_both) == {:error, :not_set}
      # :from_y should remain set
      {:ok, "Y:yval", _} = Journey.get(execution_after_unset, :from_y)
      {:ok, "yval", _} = Journey.get(execution_after_unset, :y)

      # Set :x again and verify selective recomputation
      execution_reset = Journey.set(execution_after_unset, :x, "newx")
      {:ok, "newx", _} = Journey.get(execution_reset, :x)
      {:ok, "X:newx", _} = Journey.get(execution_reset, :from_x, wait: :any)
      {:ok, "Both:newx+yval", _} = Journey.get(execution_reset, :from_both, wait: :any)
      # :from_y should still be the same
      {:ok, "Y:yval", _} = Journey.get(execution_reset, :from_y)
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "basic graph, greetings #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:first_name),
        compute(
          :greeting,
          unblocked_when({:first_name, &provided?/1}),
          fn %{first_name: first_name} ->
            {:ok, "Hello, #{first_name}"}
          end
        )
      ]
    )
  end

  describe "multiple unset |" do
    test "unset multiple values atomically" do
      graph =
        Journey.new_graph(
          "multiple unset graph #{__MODULE__} #{random_string()}",
          "1.0.0",
          [
            input(:first_name),
            input(:last_name),
            input(:email),
            compute(:full_name, [:first_name, :last_name], fn %{first_name: first, last_name: last} ->
              {:ok, "#{first} #{last}"}
            end)
          ]
        )

      execution =
        graph
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")
        |> Journey.set(:last_name, "Bros")
        |> Journey.set(:email, "mario@example.com")

      # Wait for computation
      {:ok, "Mario Bros", _} = Journey.get(execution, :full_name, wait: :any)

      original_revision = execution.revision

      # Unset multiple values atomically
      execution_after_unset = Journey.unset(execution, [:first_name, :last_name])

      # Check values are unset
      assert Journey.get(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :last_name) == {:error, :not_set}
      {:ok, "mario@example.com", _} = Journey.get(execution_after_unset, :email)
      assert Journey.get(execution_after_unset, :full_name) == {:error, :not_set}

      # Revision should be incremented (may be more than +1 due to cascading invalidation)
      assert execution_after_unset.revision > original_revision
    end

    test "unset multiple values with execution ID" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)

      # Unset using execution ID
      execution_after_unset = Journey.unset(execution.id, [:first_name])

      assert Journey.get(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :greeting) == {:error, :not_set}
    end

    test "unset multiple values - idempotent behavior" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      original_revision = execution.revision

      # Unset value that's already unset
      execution_after_unset = Journey.unset(execution, [:first_name, :first_name])

      # Revision should be incremented (may be more than +1 due to cascading invalidation)
      assert execution_after_unset.revision > original_revision

      # Unsetting already unset values should not change revision
      execution_after_second_unset = Journey.unset(execution_after_unset, [:first_name])
      assert execution_after_second_unset.revision == execution_after_unset.revision
    end

    test "unset empty list should be no-op" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      _original_revision = execution.revision

      # This should raise a function clause error due to the guard `node_names != []`
      assert_raise FunctionClauseError, fn ->
        Journey.unset(execution, [])
      end
    end

    test "unset with invalid node names" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      # Should raise error for non-existent node
      assert_raise RuntimeError, ~r/is not a valid input node/, fn ->
        Journey.unset(execution, [:nonexistent_node])
      end

      # Should raise error for non-atom node name
      assert_raise ArgumentError, ~r/All node names must be atoms/, fn ->
        Journey.unset(execution, ["string_node"])
      end
    end

    test "unset multiple values cascades correctly" do
      # Complex dependency graph
      graph =
        Journey.new_graph(
          "complex cascade graph #{__MODULE__} #{random_string()}",
          "1.0.0",
          [
            input(:a),
            input(:b),
            input(:c),
            compute(:ab, [:a, :b], fn %{a: a, b: b} -> {:ok, "#{a}+#{b}"} end),
            compute(:bc, [:b, :c], fn %{b: b, c: c} -> {:ok, "#{b}+#{c}"} end),
            compute(:abc, [:ab, :bc], fn %{ab: ab, bc: bc} -> {:ok, "#{ab}+#{bc}"} end)
          ]
        )

      execution =
        graph
        |> Journey.start_execution()
        |> Journey.set(:a, "A")
        |> Journey.set(:b, "B")
        |> Journey.set(:c, "C")

      # Wait for all computations
      {:ok, "A+B", _} = Journey.get(execution, :ab, wait: :any)
      {:ok, "B+C", _} = Journey.get(execution, :bc, wait: :any)
      {:ok, "A+B+B+C", _} = Journey.get(execution, :abc, wait: :any)

      # Unset :a and :b should cascade appropriately
      execution_after_unset = Journey.unset(execution, [:a, :b])

      # Check what's unset
      assert Journey.get(execution_after_unset, :a) == {:error, :not_set}
      assert Journey.get(execution_after_unset, :b) == {:error, :not_set}
      {:ok, "C", _} = Journey.get(execution_after_unset, :c)

      # :ab should be unset (depends on both :a and :b)
      assert Journey.get(execution_after_unset, :ab) == {:error, :not_set}
      # :bc should be unset (depends on :b)
      assert Journey.get(execution_after_unset, :bc) == {:error, :not_set}
      # :abc should be unset (depends on :ab and :bc)
      assert Journey.get(execution_after_unset, :abc) == {:error, :not_set}
    end
  end
end
