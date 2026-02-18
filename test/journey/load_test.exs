defmodule Journey.JourneyLoadTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "load with computations filter" do
    test "filters computation records by state" do
      graph =
        Journey.new_graph(
          "load computations filter #{__MODULE__} #{random_string()}",
          "1.0.0",
          [
            input(:name),
            compute(
              :greeting,
              unblocked_when({:name, &provided?/1}),
              fn %{name: name} ->
                {:ok, "Hello, #{name}"}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)

      # Before setting input: greeting computation is :not_set
      loaded = Journey.load(execution.id)
      assert length(loaded.computations) == 1
      assert hd(loaded.computations).state == :not_set

      # Trigger computation
      Journey.set(execution, :name, "Alice")
      Journey.Test.Support.Helpers.wait_for_value(execution, :greeting, "Hello, Alice")

      # Default load: returns all computation states (including :success)
      loaded_all = Journey.load(execution.id)
      success_comps = Enum.filter(loaded_all.computations, fn c -> c.state == :success end)
      assert success_comps != []

      # Filtered load: only :not_set computations
      loaded_filtered = Journey.load(execution.id, computations: [:not_set])
      assert Enum.all?(loaded_filtered.computations, fn c -> c.state == :not_set end)

      # Filtered load excludes the :success records
      filtered_count = Enum.count(loaded_filtered.computations)
      all_count = Enum.count(loaded_all.computations)
      assert filtered_count < all_count
    end
  end

  describe "load" do
    test "sunny day" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      loaded_by_id = Journey.load(execution.id)
      loaded_by_execution = Journey.load(execution)

      assert execution == loaded_by_id
      assert execution == loaded_by_execution
    end

    test "nil" do
      assert nil == Journey.load(nil)
    end

    test "no such execution" do
      _execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      assert nil == Journey.load("no_such_execution_id")
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
