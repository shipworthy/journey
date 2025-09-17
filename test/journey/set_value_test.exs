defmodule Journey.JourneySetValueTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "set_value |" do
    test "sunny day" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()
        |> Journey.set(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
    end

    test "setting value to the same value" do
      execution_v1 =
        basic_graph(random_string())
        |> Journey.start_execution()

      execution_v1 |> Journey.set(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution_v1, :first_name, wait_any: true)
      {:ok, "Hello, Mario"} = Journey.get_value(execution_v1, :greeting, wait_any: true)
      execution_after_first_set = execution_v1 |> Journey.load()

      assert execution_after_first_set.revision > execution_v1.revision

      execution_v1 |> Journey.set(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution_v1, :first_name, wait_any: true)
      execution_after_second_set = execution_v1 |> Journey.load()
      assert execution_after_second_set.revision == execution_after_first_set.revision
    end

    test "setting value to a new value" do
      execution_v1 =
        basic_graph(random_string())
        |> Journey.start_execution()

      execution_v2 = execution_v1 |> Journey.set(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution_v1, :first_name, wait_any: true)
      {:ok, "Hello, Mario"} = Journey.get_value(execution_v1, :greeting, wait_any: true)
      assert execution_v2.revision > execution_v1.revision

      execution_v3 = execution_v1 |> Journey.set(:first_name, "Luigi")
      {:ok, "Luigi"} = Journey.get_value(execution_v3, :first_name)
      {:ok, "Hello, Luigi"} = Journey.get_value(execution_v3, :greeting, wait_new: true)
      assert execution_v3.revision > execution_v2.revision
    end

    test "sunny day, updated_at timestamps get updated" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      Process.sleep(1200)

      execution =
        execution
        |> Journey.set(:first_name, "Mario")

      assert Journey.get_value(execution, :first_name) == {:ok, "Mario"}
      {:ok, greeting} = Journey.get_value(execution, :greeting, wait_any: true)
      assert greeting == "Hello, Mario"

      execution = execution |> Journey.load()

      first_name_node = value_node(execution, :first_name)
      assert first_name_node.inserted_at < first_name_node.updated_at

      greeting_value_node = value_node(execution, :greeting)
      assert greeting_value_node.inserted_at < greeting_value_node.updated_at

      compute_node = computation_node(execution, :greeting)
      assert compute_node.inserted_at < compute_node.updated_at

      assert execution.inserted_at < execution.updated_at
    end

    def value_node(execution, node_name) do
      execution.values |> Enum.find(fn x -> x.node_name == node_name end)
    end

    def computation_node(execution, node_name) do
      execution.computations |> Enum.find(fn x -> x.node_name == node_name end)
    end

    test "unknown node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':last_name' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.set(execution, :last_name, "Bowser")
                   end
    end

    test "not an input node" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise RuntimeError,
                   "':greeting' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: [:execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.set(execution, :greeting, "Hello!")
                   end
    end

    test "set_value with execution_id string" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      updated_execution = Journey.set(execution.id, :first_name, "Mario")
      assert Journey.get_value(updated_execution, :first_name) == {:ok, "Mario"}
    end

    test "atom values are rejected" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert_raise FunctionClauseError, fn ->
        Journey.set(execution, :first_name, :atom_value)
      end

      assert_raise FunctionClauseError, fn ->
        Journey.set(execution.id, :first_name, :atom_value)
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
