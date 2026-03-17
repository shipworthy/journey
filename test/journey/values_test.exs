defmodule Journey.ValuesTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  describe "values/2 with binary execution ID" do
    test "returns same result as struct" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      values_from_struct = Journey.values(execution, reload: false)
      values_from_id = Journey.values(execution.id)

      assert values_from_struct == values_from_id
    end

    test "include_unset_as_nil option works" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      values = Journey.values(execution.id, include_unset_as_nil: true)

      assert values.first_name == "Mario"
      assert Map.has_key?(values, :age)
      assert is_nil(values.age)
    end

    test "raises ArgumentError when reload: true is passed" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise ArgumentError,
                   ~r/reload.*not supported.*binary execution ID/,
                   fn ->
                     Journey.values(execution.id, reload: true)
                   end
    end

    test "raises ArgumentError when reload: false is passed" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise ArgumentError,
                   ~r/reload.*not supported.*binary execution ID/,
                   fn ->
                     Journey.values(execution.id, reload: false)
                   end
    end
  end

  describe "values_all/2 with binary execution ID" do
    test "returns same result as struct" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      values_from_struct = Journey.values_all(execution, reload: false)
      values_from_id = Journey.values_all(execution.id)

      assert values_from_struct == values_from_id
    end

    test "raises ArgumentError when reload: true is passed" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise ArgumentError,
                   ~r/reload.*not supported.*binary execution ID/,
                   fn ->
                     Journey.values_all(execution.id, reload: true)
                   end
    end

    test "raises ArgumentError when reload: false is passed" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise ArgumentError,
                   ~r/reload.*not supported.*binary execution ID/,
                   fn ->
                     Journey.values_all(execution.id, reload: false)
                   end
    end
  end

  describe "non-existent execution ID" do
    test "values/2 raises ArgumentError" do
      assert_raise ArgumentError, ~r/execution not found/, fn ->
        Journey.values("EXEC_NONEXISTENT")
      end
    end

    test "values_all/2 raises ArgumentError" do
      assert_raise ArgumentError, ~r/execution not found/, fn ->
        Journey.values_all("EXEC_NONEXISTENT")
      end
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "values test #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:first_name),
        input(:age)
      ]
    )
  end
end
