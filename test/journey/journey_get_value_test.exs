defmodule Journey.JourneyGetValueTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "get_value" do
    test "sunny day, input, not set, non-blocking" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert Journey.get_value(execution, :first_name) == {:error, :not_set}
    end

    test "sunny day, input, set, non-blocking" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
    end

    test "sunny day, computation, set, blocking" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :greeting, wait_any: true) == {:ok, "Hello, Mario"}
    end

    test "no such node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      assert_raise RuntimeError,
                   "':no_such_node' is not a known node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid node names: [:execution_id, :first_name, :greeting, :last_updated_at].",
                   fn ->
                     Journey.get_value(execution, :no_such_node, wait_any: true) == {:ok, "Hello, Mario"}
                   end
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

  describe "get_value_revision" do
    test "returns nil for node with no value" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert Journey.get_value_revision(execution, :first_name) == nil
    end

    test "returns revision for node with value" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      revision = Journey.get_value_revision(execution, :first_name)
      assert is_integer(revision)
      assert revision > 0
    end

    test "returns nil for non-existent node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert Journey.get_value_revision(execution, :nonexistent_node) == nil
    end
  end

  describe "wait_for_revision_after" do
    test "waits for revision after specified version" do
      execution =
        revision_test_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:input_value, "first")

      # Wait for initial computation
      {:ok, _} = Journey.get_value(execution, :computed_value, wait_any: true)

      # Reload execution to get updated state
      execution = Journey.load(execution)

      # Get the current revision
      revision_before = Journey.get_value_revision(execution, :computed_value)

      # Update the input to trigger recomputation
      execution = Journey.set_value(execution, :input_value, "second")

      # Wait for a revision after the one we captured
      {:ok, new_value} = Journey.get_value(execution, :computed_value, wait_for_revision_after: revision_before)

      assert new_value == "computed: second"
    end

    test "wait_for_revision_after: nil works like wait_any: true" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      # These should be equivalent
      {:ok, value1} = Journey.get_value(execution, :greeting, wait_any: true)
      {:ok, value2} = Journey.get_value(execution, :greeting, wait_for_revision_after: nil)

      assert value1 == value2
      assert value1 == "Hello, Mario"
    end

    test "mutual exclusivity with other wait options" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise ArgumentError, fn ->
        Journey.get_value(execution, :first_name, wait_any: true, wait_for_revision_after: 1)
      end

      assert_raise ArgumentError, fn ->
        Journey.get_value(execution, :first_name, wait_new: true, wait_for_revision_after: 1)
      end

      assert_raise ArgumentError, fn ->
        Journey.get_value(execution, :first_name, wait_any: true, wait_new: true, wait_for_revision_after: 1)
      end
    end
  end

  defp revision_test_graph(test_id) do
    Journey.new_graph(
      "revision test graph #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:input_value),
        compute(
          :computed_value,
          [:input_value],
          fn %{input_value: value} ->
            {:ok, "computed: #{value}"}
          end
        )
      ]
    )
  end
end
