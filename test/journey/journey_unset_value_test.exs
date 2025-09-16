defmodule Journey.JourneyUnsetValueTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "unset_value |" do
    test "unsetting a previously set value" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

      execution_after_set = Journey.load(execution)
      original_revision = execution_after_set.revision

      # Unset the value
      execution_after_unset = Journey.unset_value(execution, :first_name)

      # Value should be not_set after unset
      assert Journey.get_value(execution_after_unset, :first_name) == {:error, :not_set}

      # Revision should be incremented
      assert execution_after_unset.revision > original_revision
    end

    test "unsetting a value that was never set" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      # Value starts as not_set
      assert Journey.get_value(execution, :first_name) == {:error, :not_set}

      original_revision = execution.revision

      # Unset a value that was never set
      execution_after_unset = Journey.unset_value(execution, :first_name)

      # Value should still be not_set
      assert Journey.get_value(execution_after_unset, :first_name) == {:error, :not_set}

      # Revision should NOT change since value was already unset
      assert execution_after_unset.revision == original_revision
    end

    test "unsetting a value multiple times" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}

      # First unset
      execution_after_first_unset = Journey.unset_value(execution, :first_name)
      assert Journey.get_value(execution_after_first_unset, :first_name) == {:error, :not_set}
      first_unset_revision = execution_after_first_unset.revision

      # Second unset (should be idempotent)
      execution_after_second_unset = Journey.unset_value(execution_after_first_unset, :first_name)
      assert Journey.get_value(execution_after_second_unset, :first_name) == {:error, :not_set}

      # Revision should not change on second unset
      assert execution_after_second_unset.revision == first_unset_revision
    end

    test "unset then set again" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

      # Unset the value
      execution_after_unset = Journey.unset_value(execution, :first_name)
      assert Journey.get_value(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :greeting) == {:error, :not_set}

      # Set a new value
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Luigi")
      assert Journey.get_value(execution_after_reset, :first_name) == {:ok, "Luigi"}
      {:ok, "Hello, Luigi"} = Journey.get_value(execution_after_reset, :greeting, wait_any: true)
    end

    test "unsetting value increments revision" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Mario"} = Journey.get_value(execution, :first_name)
      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

      original_revision = execution.revision

      # Unset the value - this should increment revision
      execution_after_unset = Journey.unset_value(execution, :first_name)

      # Revision should be incremented
      assert execution_after_unset.revision > original_revision
      assert Journey.get_value(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :greeting) == {:error, :not_set}

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Bob")
      assert execution_after_reset.revision > execution_after_unset.revision
      assert Journey.get_value(execution_after_reset, :first_name) == {:ok, "Bob"}
      {:ok, "Hello, Bob"} = Journey.get_value(execution_after_reset, :greeting, wait_any: true)
    end

    test "unknown node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':last_name' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.unset_value(execution, :last_name)
                   end
    end

    test "not an input node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':greeting' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.unset_value(execution, :greeting)
                   end
    end

    test "unset_value with execution_id uses hardcoded revision assertion" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)
      execution_after_unset = Journey.unset_value(execution.id, :first_name)
      # Revision will be higher due to cascading invalidation
      assert execution_after_unset.revision > 4
      assert Journey.get_value(execution_after_unset, :first_name) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :greeting) == {:error, :not_set}

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Peach")
      assert Journey.get_value(execution_after_reset, :first_name) == {:ok, "Peach"}
      {:ok, "Hello, Peach"} = Journey.get_value(execution_after_reset, :greeting, wait_any: true)
    end

    test "unset_value keeps last_updated_at set (not unset)" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

      values_before = Journey.values_all(execution)
      assert {:set, _timestamp} = values_before.last_updated_at

      execution_after_unset = Journey.unset_value(execution, :first_name)
      values_after = Journey.values_all(execution_after_unset)

      assert {:set, _new_timestamp} = values_after.last_updated_at
      assert values_after.first_name == :not_set
      assert values_after.greeting == :not_set

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Toad")
      assert Journey.get_value(execution_after_reset, :first_name) == {:ok, "Toad"}
      {:ok, "Hello, Toad"} = Journey.get_value(execution_after_reset, :greeting, wait_any: true)

      # Verify last_updated_at is still set and was updated
      values_after_reset = Journey.values_all(execution_after_reset)
      assert {:set, _final_timestamp} = values_after_reset.last_updated_at
    end

    test "unset_value updates last_updated_at timestamp" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

      values_before = Journey.values_all(execution)
      {:set, initial_timestamp} = values_before.last_updated_at

      Process.sleep(1000)

      execution_after_unset = Journey.unset_value(execution.id, :first_name)
      values_after = Journey.values_all(execution_after_unset)

      {:set, new_timestamp} = values_after.last_updated_at
      assert new_timestamp > initial_timestamp

      # Sleep again before re-setting
      Process.sleep(1000)

      # Set a new value and verify recomputation
      execution_after_reset = Journey.set(execution_after_unset, :first_name, "Yoshi")
      assert Journey.get_value(execution_after_reset, :first_name) == {:ok, "Yoshi"}
      {:ok, "Hello, Yoshi"} = Journey.get_value(execution_after_reset, :greeting, wait_any: true)

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
      {:ok, "B:value"} = Journey.get_value(execution, :b, wait_any: true)
      {:ok, "C:B:value"} = Journey.get_value(execution, :c, wait_any: true)

      # Unset :a should cascade to :b and :c
      execution_after_unset = Journey.unset_value(execution, :a)

      assert Journey.get_value(execution_after_unset, :a) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :b) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :c) == {:error, :not_set}

      # Setting :a again should trigger recomputation
      execution_reset = Journey.set(execution_after_unset, :a, "new")
      {:ok, "B:new"} = Journey.get_value(execution_reset, :b, wait_any: true)
      {:ok, "C:B:new"} = Journey.get_value(execution_reset, :c, wait_any: true)
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
      {:ok, "B:val"} = Journey.get_value(execution, :b, wait_any: true)
      {:ok, "C:val"} = Journey.get_value(execution, :c, wait_any: true)
      {:ok, "D:B:val+C:val"} = Journey.get_value(execution, :d, wait_any: true)

      # Unset :a should cascade to :b, :c, and :d
      execution_after_unset = Journey.unset_value(execution, :a)

      assert Journey.get_value(execution_after_unset, :a) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :b) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :c) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :d) == {:error, :not_set}

      # Set :a again and verify all computations trigger
      execution_reset = Journey.set(execution_after_unset, :a, "reset")
      assert Journey.get_value(execution_reset, :a) == {:ok, "reset"}
      {:ok, "B:reset"} = Journey.get_value(execution_reset, :b, wait_any: true)
      {:ok, "C:reset"} = Journey.get_value(execution_reset, :c, wait_any: true)
      {:ok, "D:B:reset+C:reset"} = Journey.get_value(execution_reset, :d, wait_any: true)
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
      {:ok, "X:xval"} = Journey.get_value(execution, :from_x, wait_any: true)
      {:ok, "Y:yval"} = Journey.get_value(execution, :from_y, wait_any: true)
      {:ok, "Both:xval+yval"} = Journey.get_value(execution, :from_both, wait_any: true)

      # Unset :x should cascade to :from_x and :from_both, but NOT :from_y
      execution_after_unset = Journey.unset_value(execution, :x)

      assert Journey.get_value(execution_after_unset, :x) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :from_x) == {:error, :not_set}
      assert Journey.get_value(execution_after_unset, :from_both) == {:error, :not_set}
      # :from_y should remain set
      assert Journey.get_value(execution_after_unset, :from_y) == {:ok, "Y:yval"}
      assert Journey.get_value(execution_after_unset, :y) == {:ok, "yval"}

      # Set :x again and verify selective recomputation
      execution_reset = Journey.set(execution_after_unset, :x, "newx")
      assert Journey.get_value(execution_reset, :x) == {:ok, "newx"}
      {:ok, "X:newx"} = Journey.get_value(execution_reset, :from_x, wait_any: true)
      {:ok, "Both:newx+yval"} = Journey.get_value(execution_reset, :from_both, wait_any: true)
      # :from_y should still be the same
      assert Journey.get_value(execution_reset, :from_y) == {:ok, "Y:yval"}
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
end
