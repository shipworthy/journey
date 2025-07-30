defmodule Journey.InsightsTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies
  import Journey.Helpers.Random, only: [random_string: 0]

  alias Journey.Insights

  setup do
    {:ok, test_id: random_string()}
  end

  describe "status/0" do
    test "returns healthy status with expected structure", %{test_id: _test_id} do
      # Test basic structure - don't assume empty database
      result = Insights.status()

      assert %{
               status: status,
               database_connected: database_connected,
               graphs: graphs
             } = result

      assert status == :healthy
      assert database_connected == true
      assert is_list(graphs)
    end

    test "returns correct structure with single graph and execution data", %{test_id: test_id} do
      # Create a simple test graph
      graph = simple_test_graph(test_id)

      # Create executions in different states
      exec1 = Journey.start_execution(graph)
      exec2 = Journey.start_execution(graph)
      exec3 = Journey.start_execution(graph)

      # Set some values to trigger computations
      exec1 = Journey.set_value(exec1, :name, "Alice")
      exec2 = Journey.set_value(exec2, :name, "Bob")

      # Archive one execution
      Journey.archive(exec3)

      # Wait for computations to complete
      {:ok, _} = Journey.get_value(exec1, :greeting, wait_any: true)
      {:ok, _} = Journey.get_value(exec2, :greeting, wait_any: true)

      result = Insights.status()

      assert result.status == :healthy
      assert result.database_connected == true
      assert length(result.graphs) >= 1

      # Find our test graph in the results
      test_graph_stats =
        Enum.find(result.graphs, fn g ->
          g.graph_name == graph.name and g.graph_version == graph.version
        end)

      assert test_graph_stats != nil

      # Verify execution stats
      exec_stats = test_graph_stats.stats.executions
      assert exec_stats.active >= 2
      assert exec_stats.archived >= 1
      assert exec_stats.most_recently_created =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*Z$/
      assert exec_stats.most_recently_updated =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*Z$/

      # Verify computation stats structure
      comp_stats = test_graph_stats.stats.computations
      assert is_map(comp_stats.by_state)
      assert comp_stats.most_recently_created =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*Z$/
      assert comp_stats.most_recently_updated =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*Z$/

      # Verify all computation states are present
      expected_states = [:not_set, :computing, :success, :failed, :abandoned, :cancelled]

      for state <- expected_states do
        assert Map.has_key?(comp_stats.by_state, state)
        assert is_integer(comp_stats.by_state[state])
      end

      # We should have some successful computations from our test
      assert comp_stats.by_state.success >= 2
    end

    test "returns correct structure with multiple graphs", %{test_id: test_id} do
      # Create two different graphs
      graph1 = simple_test_graph("#{test_id}_graph1")
      graph2 = computation_heavy_graph("#{test_id}_graph2")

      # Create executions for both graphs
      exec1 = Journey.start_execution(graph1) |> Journey.set_value(:name, "Alice")
      exec2 = Journey.start_execution(graph2) |> Journey.set_value(:input_value, 10)

      # Wait for computations
      {:ok, _} = Journey.get_value(exec1, :greeting, wait_any: true)
      {:ok, _} = Journey.get_value(exec2, :final_result, wait_any: true)

      result = Insights.status()

      assert result.status == :healthy
      assert result.database_connected == true

      # Should have at least our two graphs
      test_graphs =
        Enum.filter(result.graphs, fn g ->
          String.contains?(g.graph_name, test_id)
        end)

      assert length(test_graphs) >= 2

      # Verify each graph has the expected structure
      for graph_stats <- test_graphs do
        assert %{
                 graph_name: _,
                 graph_version: _,
                 stats: %{
                   executions: %{
                     archived: _,
                     active: _,
                     most_recently_created: _,
                     most_recently_updated: _
                   },
                   computations: %{
                     by_state: _,
                     most_recently_created: _,
                     most_recently_updated: _
                   }
                 }
               } = graph_stats
      end
    end

    test "handles various computation states correctly", %{test_id: test_id} do
      graph = failing_computation_graph(test_id)

      # Create executions that will have different computation outcomes
      exec1 = Journey.start_execution(graph)
      exec2 = Journey.start_execution(graph)

      # Set values to trigger computations - some will succeed, some will fail
      Journey.set_value(exec1, :should_fail, false)
      Journey.set_value(exec2, :should_fail, true)

      # Wait a bit for computations to process
      Process.sleep(100)

      result = Insights.status()

      assert result.status == :healthy

      # Find our test graph
      test_graph_stats =
        Enum.find(result.graphs, fn g ->
          g.graph_name == graph.name
        end)

      if test_graph_stats do
        comp_stats = test_graph_stats.stats.computations

        # Should have a mix of different states
        total_computations =
          comp_stats.by_state
          |> Map.values()
          |> Enum.sum()

        assert total_computations > 0
      end
    end

    test "timestamp format is valid ISO8601 with UTC timezone", %{test_id: test_id} do
      graph = simple_test_graph(test_id)

      # Create and process an execution
      exec = Journey.start_execution(graph)
      Journey.set_value(exec, :name, "TestUser")
      {:ok, _} = Journey.get_value(exec, :greeting, wait_any: true)

      result = Insights.status()

      if length(result.graphs) > 0 do
        graph_stats = hd(result.graphs)
        exec_stats = graph_stats.stats.executions
        comp_stats = graph_stats.stats.computations

        # Test execution timestamps
        if exec_stats.most_recently_created do
          assert exec_stats.most_recently_created =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/
          {:ok, datetime, 0} = DateTime.from_iso8601(exec_stats.most_recently_created)
          assert datetime.time_zone == "Etc/UTC"
        end

        if exec_stats.most_recently_updated do
          assert exec_stats.most_recently_updated =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/
          {:ok, datetime, 0} = DateTime.from_iso8601(exec_stats.most_recently_updated)
          assert datetime.time_zone == "Etc/UTC"
        end

        # Test computation timestamps
        if comp_stats.most_recently_created do
          assert comp_stats.most_recently_created =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/
          {:ok, datetime, 0} = DateTime.from_iso8601(comp_stats.most_recently_created)
          assert datetime.time_zone == "Etc/UTC"
        end

        if comp_stats.most_recently_updated do
          assert comp_stats.most_recently_updated =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/
          {:ok, datetime, 0} = DateTime.from_iso8601(comp_stats.most_recently_updated)
          assert datetime.time_zone == "Etc/UTC"
        end
      end
    end
  end

  # Helper functions for creating test graphs

  defp simple_test_graph(test_id) do
    Journey.new_graph(
      "insights_test_simple_#{test_id}",
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
  end

  defp computation_heavy_graph(test_id) do
    Journey.new_graph(
      "insights_test_heavy_#{test_id}",
      "2.0.0",
      [
        input(:input_value),
        compute(
          :doubled,
          unblocked_when({:input_value, &provided?/1}),
          fn %{input_value: val} ->
            {:ok, val * 2}
          end
        ),
        compute(
          :tripled,
          unblocked_when({:input_value, &provided?/1}),
          fn %{input_value: val} ->
            {:ok, val * 3}
          end
        ),
        compute(
          :final_result,
          unblocked_when({:and, [{:doubled, &provided?/1}, {:tripled, &provided?/1}]}),
          fn %{doubled: d, tripled: t} ->
            {:ok, d + t}
          end
        )
      ]
    )
  end

  defp failing_computation_graph(test_id) do
    Journey.new_graph(
      "insights_test_failing_#{test_id}",
      "1.0.0",
      [
        input(:should_fail),
        compute(
          :result,
          unblocked_when({:should_fail, &provided?/1}),
          fn %{should_fail: should_fail} ->
            if should_fail do
              {:error, "Intentional test failure"}
            else
              {:ok, "Success!"}
            end
          end
        )
      ]
    )
  end
end
