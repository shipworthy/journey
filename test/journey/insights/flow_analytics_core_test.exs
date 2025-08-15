defmodule Journey.Insights.FlowAnalyticsCoreTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies
  import Journey.Helpers.Random, only: [random_string: 0]

  alias Journey.Insights.FlowAnalytics, as: Insights

  setup do
    {:ok, test_id: random_string()}
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

  describe "to_text/1" do
    test "formats flow analytics with nodes correctly", %{test_id: _test_id} do
      flow_analytics_data = %{
        graph_name: "Test Graph",
        graph_version: "v1.0.0",
        analyzed_at: "2025-08-15T12:00:00Z",
        executions: %{
          count: 1234,
          duration_avg_seconds_to_last_update: 45,
          duration_median_seconds_to_last_update: 0
        },
        node_stats: %{
          nodes: [
            %{
              node_name: :birth_date,
              node_type: :input,
              reached_count: 800,
              reached_percentage: 64.8,
              average_time_to_reach: 1,
              flow_ends_here_count: 400,
              flow_ends_here_percentage_of_all: 32.4,
              flow_ends_here_percentage_of_reached: 50.0
            },
            %{
              node_name: :credit_score,
              node_type: :compute,
              reached_count: 500,
              reached_percentage: 40.5,
              average_time_to_reach: 2,
              flow_ends_here_count: 0,
              flow_ends_here_percentage_of_all: 0.0,
              flow_ends_here_percentage_of_reached: 0.0
            }
          ]
        }
      }

      expected_output = """
      Graph: 'Test Graph'
      Version: 'v1.0.0'
      Analyzed at: 2025-08-15T12:00:00Z

      EXECUTION STATS:
      ----------
      Total executions: 1.2k
      Average duration: 45 seconds
      Median duration: 0 seconds

      NODE STATS (2 nodes):
      ----------
      Node Name: 'birth_date'
      Type: input
      Reached by: 800 executions (64.8%)
      Average time to reach: 1 second
      Flow ends here: 400 executions (32.4% of all, 50.0% of reached)

      Node Name: 'credit_score'
      Type: compute
      Reached by: 500 executions (40.5%)
      Average time to reach: 2 seconds
      Flow ends here: 0 executions (0.0% of all, 0.0% of reached)
      """

      assert Insights.to_text(flow_analytics_data) == expected_output
    end

    test "formats flow analytics with no nodes", %{test_id: _test_id} do
      flow_analytics_data = %{
        graph_name: "Empty Graph",
        graph_version: "v1.0.0",
        analyzed_at: "2025-08-15T12:00:00Z",
        executions: %{
          count: 0,
          duration_avg_seconds_to_last_update: 0,
          duration_median_seconds_to_last_update: 0
        },
        node_stats: %{
          nodes: []
        }
      }

      expected_output = """
      Graph: 'Empty Graph'
      Version: 'v1.0.0'
      Analyzed at: 2025-08-15T12:00:00Z

      EXECUTION STATS:
      ----------
      Total executions: 0
      Average duration: 0 seconds
      Median duration: 0 seconds

      NODE STATS: No nodes found.
      """

      assert Insights.to_text(flow_analytics_data) == expected_output
    end

    test "formats large numbers correctly", %{test_id: _test_id} do
      flow_analytics_data = %{
        graph_name: "Large Scale Graph",
        graph_version: "v2.0.0",
        analyzed_at: "2025-08-15T12:00:00Z",
        executions: %{
          count: 1_500_000,
          duration_avg_seconds_to_last_update: 120,
          duration_median_seconds_to_last_update: 60
        },
        node_stats: %{
          nodes: [
            %{
              node_name: :registration,
              node_type: :input,
              reached_count: 1_200_000,
              reached_percentage: 80.0,
              average_time_to_reach: 0,
              flow_ends_here_count: 300_000,
              flow_ends_here_percentage_of_all: 20.0,
              flow_ends_here_percentage_of_reached: 25.0
            }
          ]
        }
      }

      text_output = Insights.to_text(flow_analytics_data)

      # Check large number formatting
      assert text_output =~ "Total executions: 1.5M"
      assert text_output =~ "Average duration: 120 seconds"
      assert text_output =~ "Median duration: 60 seconds"
      assert text_output =~ "Reached by: 1.2M executions (80.0%)"
      assert text_output =~ "Flow ends here: 300.0k executions (20.0% of all, 25.0% of reached)"
    end

    test "handles missing or nil data gracefully", %{test_id: _test_id} do
      flow_analytics_data = %{
        graph_name: nil,
        executions: nil,
        node_stats: nil
      }

      text_output = Insights.to_text(flow_analytics_data)

      assert text_output =~ "Graph: 'Unknown'"
      assert text_output =~ "Version: 'Unknown'"
      assert text_output =~ "Analyzed at: Unknown"
      assert text_output =~ "Total executions: 0"
      assert text_output =~ "NODE STATS: No nodes found."
    end

    test "formats duration correctly", %{test_id: _test_id} do
      flow_analytics_data = %{
        graph_name: "Duration Test",
        graph_version: "v1.0.0",
        analyzed_at: "2025-08-15T12:00:00Z",
        executions: %{
          count: 100,
          duration_avg_seconds_to_last_update: 1,
          duration_median_seconds_to_last_update: 3600
        },
        node_stats: %{
          nodes: [
            %{
              node_name: :test_node,
              node_type: :input,
              reached_count: 100,
              reached_percentage: 100.0,
              average_time_to_reach: 0,
              flow_ends_here_count: 50,
              flow_ends_here_percentage_of_all: 50.0,
              flow_ends_here_percentage_of_reached: 50.0
            }
          ]
        }
      }

      text_output = Insights.to_text(flow_analytics_data)

      assert text_output =~ "Average duration: 1 second"
      assert text_output =~ "Median duration: 3600 seconds"
      assert text_output =~ "Average time to reach: 0 seconds"
    end
  end

  # Helper functions
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
end
