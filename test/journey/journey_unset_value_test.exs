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
        |> Journey.set_value(:first_name, "Mario")

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
        |> Journey.set_value(:first_name, "Mario")

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
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

      # Unset the value
      execution_after_unset = Journey.unset_value(execution, :first_name)
      assert Journey.get_value(execution_after_unset, :first_name) == {:error, :not_set}

      # Set a new value
      execution_after_reset = Journey.set_value(execution_after_unset, :first_name, "Luigi")
      assert Journey.get_value(execution_after_reset, :first_name) == {:ok, "Luigi"}
      {:ok, "Hello, Luigi"} = Journey.get_value(execution_after_reset, :greeting, wait_new: true)
    end

    test "unsetting value increments revision" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")

      {:ok, "Mario"} = Journey.get_value(execution, :first_name)
      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

      original_revision = execution.revision

      # Unset the value - this should increment revision
      execution_after_unset = Journey.unset_value(execution, :first_name)

      # Revision should be incremented even though computation may not re-run
      assert execution_after_unset.revision > original_revision
      assert Journey.get_value(execution_after_unset, :first_name) == {:error, :not_set}
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
