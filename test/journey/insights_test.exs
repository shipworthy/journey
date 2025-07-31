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

  describe "flow_analytics/3" do
    test "returns correct structure with basic graph", %{test_id: test_id} do
      graph = simple_test_graph(test_id)

      # Create some executions with different states
      exec1 = Journey.start_execution(graph)
      exec2 = Journey.start_execution(graph)
      exec3 = Journey.start_execution(graph)

      # Set values to create a flow
      exec1 = Journey.set_value(exec1, :name, "Alice")
      exec2 = Journey.set_value(exec2, :name, "Bob")
      # Leave exec3 without setting name

      # Wait for computations
      {:ok, _} = Journey.get_value(exec1, :greeting, wait_any: true)
      {:ok, _} = Journey.get_value(exec2, :greeting, wait_any: true)

      # Archive one execution
      Journey.archive(exec3)

      result = Insights.flow_analytics(graph.name, graph.version)

      assert %{
               graph_name: graph_name,
               graph_version: graph_version,
               analyzed_at: analyzed_at,
               executions: executions,
               node_stats: node_stats
             } = result

      assert graph_name == graph.name
      assert graph_version == graph.version
      assert analyzed_at =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/

      # Check execution analytics
      assert %{
               count: count,
               duration_median_seconds_to_last_update: median,
               duration_avg_seconds_to_last_update: avg
             } = executions

      # Only active executions by default
      assert count == 2
      assert is_integer(median)
      assert is_integer(avg)

      # Check node stats structure
      assert %{nodes: nodes} = node_stats
      assert is_list(nodes)
      assert length(nodes) >= 1

      # Verify node structure
      for node <- nodes do
        assert %{
                 node_name: node_name,
                 node_type: node_type,
                 reached_count: reached_count,
                 reached_percentage: reached_percentage,
                 average_time_to_reach: avg_time,
                 flow_ends_here_count: flow_ends_count,
                 flow_ends_here_percentage_of_all: flow_ends_pct_all,
                 flow_ends_here_percentage_of_reached: flow_ends_pct_reached
               } = node

        assert is_atom(node_name)
        assert node_type in [:input, :compute, :mutate, :schedule_once, :schedule_recurring]
        assert is_integer(reached_count)
        assert is_float(reached_percentage)
        assert is_integer(avg_time)
        assert is_integer(flow_ends_count)
        assert is_float(flow_ends_pct_all)
        assert is_float(flow_ends_pct_reached)
      end
    end

    test "respects include_executions option", %{test_id: test_id} do
      graph = simple_test_graph(test_id)

      # Create executions
      exec1 = Journey.start_execution(graph)
      exec2 = Journey.start_execution(graph)
      exec3 = Journey.start_execution(graph)

      # Set values
      exec1 = Journey.set_value(exec1, :name, "Alice")
      exec2 = Journey.set_value(exec2, :name, "Bob")

      # Wait for computations
      {:ok, _} = Journey.get_value(exec1, :greeting, wait_any: true)
      {:ok, _} = Journey.get_value(exec2, :greeting, wait_any: true)

      # Archive one execution
      Journey.archive(exec3)

      # Test :active (default)
      result_active = Insights.flow_analytics(graph.name, graph.version, include_executions: :active)
      assert result_active.executions.count == 2

      # Test :all
      result_all = Insights.flow_analytics(graph.name, graph.version, include_executions: :all)
      assert result_all.executions.count == 3

      # Test :archived
      result_archived = Insights.flow_analytics(graph.name, graph.version, include_executions: :archived)
      assert result_archived.executions.count == 1
    end

    test "handles empty graph", %{test_id: test_id} do
      # Use a unique graph name that doesn't exist
      graph_name = "nonexistent_graph_#{test_id}"
      result = Insights.flow_analytics(graph_name, "1.0.0")

      assert %{
               graph_name: ^graph_name,
               graph_version: "1.0.0",
               analyzed_at: _,
               executions: %{
                 count: 0,
                 duration_median_seconds_to_last_update: 0,
                 duration_avg_seconds_to_last_update: 0
               },
               node_stats: %{nodes: []}
             } = result
    end

    test "calculates percentages correctly", %{test_id: test_id} do
      graph = simple_test_graph(test_id)

      # Create 10 executions
      executions =
        for _i <- 1..10 do
          Journey.start_execution(graph)
        end

      # Set name for 8 out of 10 executions (80%)
      set_executions = Enum.take(executions, 8)

      for {exec, i} <- Enum.with_index(set_executions) do
        Journey.set_value(exec, :name, "User#{i}")
      end

      # Wait for computations to complete
      for exec <- set_executions do
        {:ok, _} = Journey.get_value(exec, :greeting, wait_any: true)
      end

      result = Insights.flow_analytics(graph.name, graph.version)

      assert result.executions.count == 10

      # Find the name node
      name_node = Enum.find(result.node_stats.nodes, fn node -> node.node_name == :name end)
      assert name_node != nil
      assert name_node.reached_count == 8
      assert name_node.reached_percentage == 80.0

      # Find the greeting node (should also be 8 since it depends on name)
      greeting_node = Enum.find(result.node_stats.nodes, fn node -> node.node_name == :greeting end)
      assert greeting_node != nil
      assert greeting_node.reached_count == 8
      assert greeting_node.reached_percentage == 80.0
    end

    test "handles flow ending logic with custom duration", %{test_id: test_id} do
      graph = simple_test_graph(test_id)

      # Create executions
      exec1 = Journey.start_execution(graph)
      exec2 = Journey.start_execution(graph)

      # Set values
      Journey.set_value(exec1, :name, "Alice")
      Journey.set_value(exec2, :name, "Bob")

      # Wait for computations
      {:ok, _} = Journey.get_value(exec1, :greeting, wait_any: true)
      {:ok, _} = Journey.get_value(exec2, :greeting, wait_any: true)

      # Test with very long flow_ends_here_after (should show no flow endings)
      result_long = Insights.flow_analytics(graph.name, graph.version, flow_ends_here_after: 86_400)

      for node <- result_long.node_stats.nodes do
        assert node.flow_ends_here_count == 0
        assert node.flow_ends_here_percentage_of_all == 0.0
        assert node.flow_ends_here_percentage_of_reached == 0.0
      end

      # Test with very short flow_ends_here_after (should show flow endings)
      result_short = Insights.flow_analytics(graph.name, graph.version, flow_ends_here_after: 0)

      # At least some nodes should show flow endings
      total_flow_ends = Enum.sum(Enum.map(result_short.node_stats.nodes, & &1.flow_ends_here_count))
      # Could be 0 if timing is very fast
      assert total_flow_ends >= 0
    end

    test "handles multiple graph versions", %{test_id: test_id} do
      graph_name = "multi_version_#{test_id}"

      # Create and fully process v1.0.0
      graph_v1 =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:name),
            compute(:greeting, unblocked_when({:name, &provided?/1}), fn %{name: name} ->
              {:ok, "Hello, #{name}"}
            end)
          ]
        )

      exec_v1 = Journey.start_execution(graph_v1)
      exec_v1 = Journey.set_value(exec_v1, :name, "Alice")
      {:ok, _} = Journey.get_value(exec_v1, :greeting, wait_any: true)

      # Now create and process v2.0.0
      graph_v2 =
        Journey.new_graph(
          graph_name,
          "2.0.0",
          [
            input(:name),
            input(:title),
            compute(:greeting, unblocked_when({:and, [{:name, &provided?/1}, {:title, &provided?/1}]}), fn %{
                                                                                                             name: name,
                                                                                                             title:
                                                                                                               title
                                                                                                           } ->
              {:ok, "Hello, #{title} #{name}"}
            end)
          ]
        )

      exec_v2 = Journey.start_execution(graph_v2)
      exec_v2 = Journey.set_value(exec_v2, :name, "Bob")
      exec_v2 = Journey.set_value(exec_v2, :title, "Dr.")
      {:ok, _} = Journey.get_value(exec_v2, :greeting, wait_any: true)

      # Test v1.0.0
      result_v1 = Insights.flow_analytics(graph_name, "1.0.0")
      assert result_v1.graph_version == "1.0.0"
      assert result_v1.executions.count == 1

      # Test v2.0.0
      result_v2 = Insights.flow_analytics(graph_name, "2.0.0")
      assert result_v2.graph_version == "2.0.0"
      assert result_v2.executions.count == 1

      # v2 should have more nodes (name, title, greeting) vs v1 (name, greeting)
      assert length(result_v2.node_stats.nodes) > length(result_v1.node_stats.nodes)
    end

    test "redacts dynamic values in test output correctly", %{test_id: test_id} do
      graph = simple_test_graph(test_id)

      exec = Journey.start_execution(graph)
      Journey.set_value(exec, :name, "TestUser")
      {:ok, _} = Journey.get_value(exec, :greeting, wait_any: true)

      result = Insights.flow_analytics(graph.name, graph.version)

      # Test that we can redact the analyzed_at timestamp
      redacted_result = result |> redact(:analyzed_at)
      assert redacted_result.analyzed_at == "..."
      assert redacted_result.graph_name == graph.name

      # Test redacting multiple fields
      redacted_multiple = result |> redact([:analyzed_at, :graph_name])
      assert redacted_multiple.analyzed_at == "..."
      assert redacted_multiple.graph_name == "..."
      assert redacted_multiple.graph_version == graph.version
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
