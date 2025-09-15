defmodule Journey.JourneySetValuesTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node

  describe "set_values |" do
    test "basic multi-value setting" do
      execution =
        multi_input_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_values(%{first_name: "Mario", last_name: "Bros"})

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
      assert Journey.get_value(execution, :last_name) == {:ok, "Bros"}
      assert Journey.get_value(execution, :full_name, wait_any: true) == {:ok, "Mario Bros"}
    end

    test "empty map should be no-op" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      original_revision = execution.revision
      execution = Journey.set_values(execution, %{})

      assert execution.revision == original_revision
    end

    test "setting values that are all unchanged should be no-op" do
      execution_v1 =
        multi_input_graph(random_string())
        |> Journey.start_execution()

      # Set initial values
      execution_v2 = Journey.set_values(execution_v1, %{first_name: "Mario", last_name: "Bros"})
      assert execution_v2.revision > execution_v1.revision

      # Set same values again - should be no-op
      execution_v3 = Journey.set_values(execution_v2, %{first_name: "Mario", last_name: "Bros"})
      assert execution_v3.revision == execution_v2.revision
    end

    test "partial updates - only changed values increment revision once" do
      execution_v1 =
        multi_input_graph(random_string())
        |> Journey.start_execution()

      # Set initial values
      execution_v2 = Journey.set_values(execution_v1, %{first_name: "Mario", last_name: "Bros"})

      # Wait for computation to complete and get final revision
      {:ok, "Mario Bros"} = Journey.get_value(execution_v2, :full_name, wait_any: true)
      execution_v2_final = Journey.load(execution_v2.id)

      # Change only one value, keep one the same
      execution_v3 = Journey.set_values(execution_v2_final, %{first_name: "Luigi", last_name: "Bros"})

      # Should increment revision exactly once more for the change
      assert execution_v3.revision == execution_v2_final.revision + 1
      assert Journey.get_value(execution_v3, :first_name) == {:ok, "Luigi"}
      assert Journey.get_value(execution_v3, :last_name) == {:ok, "Bros"}
    end

    test "atomic updates with compute node dependency" do
      execution =
        multi_input_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_values(%{first_name: "Mario", last_name: "Bros", age: 35})

      # All values should be set and computed value should be available
      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
      assert Journey.get_value(execution, :last_name) == {:ok, "Bros"}
      assert Journey.get_value(execution, :age) == {:ok, 35}
      assert Journey.get_value(execution, :full_name, wait_any: true) == {:ok, "Mario Bros"}
    end

    test "different value types" do
      execution =
        value_types_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_values(%{
          text: "hello",
          number: 42,
          flag: true,
          data: %{key: "value"},
          items: [1, 2, 3]
        })

      assert Journey.get_value(execution, :text) == {:ok, "hello"}
      assert Journey.get_value(execution, :number) == {:ok, 42}
      assert Journey.get_value(execution, :flag) == {:ok, true}
      assert Journey.get_value(execution, :data) == {:ok, %{"key" => "value"}}
      assert Journey.get_value(execution, :items) == {:ok, [1, 2, 3]}
    end

    test "using execution ID string" do
      execution = basic_graph(random_string()) |> Journey.start_execution()

      updated_execution = Journey.set_values(execution.id, %{first_name: "Mario"})

      assert Journey.get_value(updated_execution, :first_name) == {:ok, "Mario"}
    end

    test "invalidates dependent computations correctly" do
      execution =
        multi_input_graph(random_string())
        |> Journey.start_execution()

      # Set initial values and let computation run
      execution = Journey.set_values(execution, %{first_name: "Mario", last_name: "Bros"})
      {:ok, "Mario Bros"} = Journey.get_value(execution, :full_name, wait_any: true)

      # Change values - should invalidate and recompute
      execution = Journey.set_values(execution, %{first_name: "Luigi", last_name: "Mario"})
      {:ok, "Luigi Mario"} = Journey.get_value(execution, :full_name, wait_new: true)
    end

    test "timestamps get updated correctly" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      Process.sleep(1200)

      execution = Journey.set_values(execution, %{first_name: "Mario"})

      execution = execution |> Journey.load()

      first_name_node = value_node(execution, :first_name)
      assert first_name_node.inserted_at < first_name_node.updated_at

      assert execution.inserted_at < execution.updated_at
    end

    test "error: unknown node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   ~r"':unknown_node' is not a valid input node in execution",
                   fn ->
                     Journey.set_values(execution, %{unknown_node: "value"})
                   end
    end

    test "error: invalid value type" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise ArgumentError,
                   ~r"Invalid value type for node first_name:",
                   fn ->
                     Journey.set_values(execution, %{first_name: {:invalid, :tuple}})
                   end
    end

    test "error: non-atom node name" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise ArgumentError,
                   "Node names must be atoms, got: \"first_name\"",
                   fn ->
                     Journey.set_values(execution, %{"first_name" => "Mario"})
                   end
    end

    test "performance: single revision increment for multiple values" do
      execution =
        multi_input_graph(random_string())
        |> Journey.start_execution()

      starting_revision = execution.revision

      # Set 3 values at once
      execution =
        Journey.set_values(execution, %{
          first_name: "Mario",
          last_name: "Bros",
          age: 35
        })

      # Should increment revision exactly once
      assert execution.revision == starting_revision + 1
    end

    test "consistency: same result as individual set_value calls" do
      # Test that set_values produces same final state as individual calls
      execution1 =
        multi_input_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set_values(%{first_name: "Mario", last_name: "Bros", age: 35})

      execution2 =
        multi_input_graph(execution1.graph_name)
        |> Journey.start_execution()
        |> Journey.set_value(:first_name, "Mario")
        |> Journey.set_value(:last_name, "Bros")
        |> Journey.set_value(:age, 35)

      # Should have same final values
      assert Journey.get_value(execution1, :first_name) == Journey.get_value(execution2, :first_name)
      assert Journey.get_value(execution1, :last_name) == Journey.get_value(execution2, :last_name)
      assert Journey.get_value(execution1, :age) == Journey.get_value(execution2, :age)

      # Computed values should be the same
      {:ok, full_name1} = Journey.get_value(execution1, :full_name, wait_any: true)
      {:ok, full_name2} = Journey.get_value(execution2, :full_name, wait_any: true)
      assert full_name1 == full_name2

      # But revision should be lower for batch operation
      assert execution1.revision < execution2.revision
    end

    def value_node(execution, node_name) do
      execution.values |> Enum.find(fn x -> x.node_name == node_name end)
    end

    def basic_graph(graph_name) do
      Journey.new_graph(
        graph_name,
        "v1.0.0",
        [
          input(:first_name),
          compute(:greeting, [:first_name], fn %{first_name: name} ->
            {:ok, "Hello, #{name}"}
          end)
        ]
      )
    end

    def multi_input_graph(graph_name) do
      Journey.new_graph(
        graph_name,
        "v1.0.0",
        [
          input(:first_name),
          input(:last_name),
          input(:age),
          compute(:full_name, [:first_name, :last_name], fn %{first_name: first, last_name: last} ->
            {:ok, "#{first} #{last}"}
          end)
        ]
      )
    end

    def value_types_graph(graph_name) do
      Journey.new_graph(
        graph_name,
        "v1.0.0",
        [
          input(:text),
          input(:number),
          input(:flag),
          input(:data),
          input(:items)
        ]
      )
    end
  end
end
