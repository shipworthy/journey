defmodule Journey.ValuesIncludeUnsetTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  describe "values with include_unset_as_nil option" do
    test "default behavior unchanged - only returns set values" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      # Default behavior should not include unset nodes
      values = Journey.values(execution)

      assert Map.has_key?(values, :first_name)
      assert values.first_name == "Mario"
      assert not Map.has_key?(values, :age)

      # Should include metadata
      assert Map.has_key?(values, :execution_id)
      assert Map.has_key?(values, :last_updated_at)
    end

    test "include_unset_as_nil: false has same behavior as default" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      default_values = Journey.values(execution)
      explicit_false_values = Journey.values(execution, include_unset_as_nil: false)

      assert default_values == explicit_false_values
    end

    test "include_unset_as_nil: true returns all nodes with nil for unset" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      values = Journey.values(execution, include_unset_as_nil: true)

      # Set values should appear unwrapped
      assert values.first_name == "Mario"

      # Unset values should appear as nil
      assert Map.has_key?(values, :age)
      assert is_nil(values.age)

      # Should include metadata
      assert Map.has_key?(values, :execution_id)
      assert Map.has_key?(values, :last_updated_at)
      assert not is_nil(values.execution_id)
      assert not is_nil(values.last_updated_at)
    end

    test "include_unset_as_nil: true with no values set" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      values = Journey.values(execution, include_unset_as_nil: true)

      # All user nodes should be nil
      assert Map.has_key?(values, :first_name)
      assert is_nil(values.first_name)
      assert Map.has_key?(values, :age)
      assert is_nil(values.age)

      # Metadata should still be set
      assert not is_nil(values.execution_id)
      assert not is_nil(values.last_updated_at)
    end

    test "include_unset_as_nil: true with all values set" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")
        |> Journey.set(:age, 35)

      values = Journey.values(execution, include_unset_as_nil: true)

      # All values should be unwrapped
      assert values.first_name == "Mario"
      assert values.age == 35
      assert not is_nil(values.execution_id)
      assert not is_nil(values.last_updated_at)
    end

    test "include_unset_as_nil: true preserves nil values when explicitly set" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, nil)

      values = Journey.values(execution, include_unset_as_nil: true)

      # Explicitly set nil should remain nil
      assert Map.has_key?(values, :first_name)
      assert is_nil(values.first_name)

      # Unset value should also be nil
      assert Map.has_key?(values, :age)
      assert is_nil(values.age)
    end

    test "reload option works with include_unset_as_nil" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      # Test with reload: true (default)
      values1 = Journey.values(execution, include_unset_as_nil: true)

      # Test with reload: false
      values2 = Journey.values(execution, include_unset_as_nil: true, reload: false)

      # Should have same structure and content
      assert Map.keys(values1) == Map.keys(values2)
      assert values1.first_name == values2.first_name
      assert values1.age == values2.age
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "values include unset test #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:first_name),
        input(:age)
      ]
    )
  end
end
