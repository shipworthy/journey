defmodule Journey.InsightsTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies
  import Journey.Helpers.Random, only: [random_string: 0]

  alias Journey.Insights.Status

  setup do
    {:ok, test_id: random_string()}
  end

  describe "status/0" do
    test "returns healthy status with expected structure", %{test_id: _test_id} do
      # Test basic structure - don't assume empty database
      result = Status.status()

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
      exec1 = Journey.set(exec1, :name, "Alice")
      exec2 = Journey.set(exec2, :name, "Bob")

      # Archive one execution
      Journey.archive(exec3)

      # Wait for computations to complete
      {:ok, %{value: _}} = Journey.get_value(exec1, :greeting, wait_any: true)
      {:ok, %{value: _}} = Journey.get_value(exec2, :greeting, wait_any: true)

      result = Status.status()

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
      exec1 = Journey.start_execution(graph1) |> Journey.set(:name, "Alice")
      exec2 = Journey.start_execution(graph2) |> Journey.set(:input_value, 10)

      # Wait for computations
      {:ok, %{value: _}} = Journey.get_value(exec1, :greeting, wait_any: true)
      {:ok, %{value: _}} = Journey.get_value(exec2, :final_result, wait_any: true)

      result = Status.status()

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
      Journey.set(exec1, :should_fail, false)
      Journey.set(exec2, :should_fail, true)

      # Wait a bit for computations to process
      Process.sleep(100)

      result = Status.status()

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
      Journey.set(exec, :name, "TestUser")
      {:ok, %{value: _}} = Journey.get_value(exec, :greeting, wait_any: true)

      result = Status.status()

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

  describe "to_text/1" do
    test "formats healthy status with active graphs", %{test_id: test_id} do
      # Create test data
      graph = simple_test_graph(test_id)
      exec = Journey.start_execution(graph)
      Journey.set(exec, :name, "Test")
      {:ok, %{value: _}} = Journey.get_value(exec, :greeting, wait_any: true)

      status_data = Status.status()
      text_output = Status.to_text(status_data)

      # Check header
      assert text_output =~ "System Status: HEALTHY"
      assert text_output =~ "Database: Connected"
      assert text_output =~ "=" |> String.duplicate(80)

      # Check for active graphs section if there are any
      active_graphs =
        Enum.filter(status_data.graphs, fn g ->
          (g[:stats][:executions][:active] || 0) > 0
        end)

      if length(active_graphs) > 0 do
        assert text_output =~ "GRAPHS"
        assert text_output =~ "----------"
      end
    end

    test "formats status with no graphs", %{test_id: _test_id} do
      status_data = %{
        status: :healthy,
        database_connected: true,
        graphs: []
      }

      expected_output = """
      System Status: HEALTHY
      Database: Connected
      ================================================================================

      No graphs found in system.
      """

      assert Status.to_text(status_data) == expected_output
    end

    test "formats unhealthy status with database disconnected", %{test_id: _test_id} do
      status_data = %{
        status: :unhealthy,
        database_connected: false,
        graphs: []
      }

      expected_output = """
      System Status: UNHEALTHY
      Database: DISCONNECTED
      ================================================================================

      No graphs found in system.
      """

      assert Status.to_text(status_data) == expected_output
    end

    test "formats graph with computation states", %{test_id: test_id} do
      status_data = %{
        status: :healthy,
        database_connected: true,
        graphs: [
          %{
            graph_name: "Test Graph #{test_id}",
            graph_version: "v1.0.0",
            stats: %{
              executions: %{
                active: 1234,
                archived: 567,
                most_recently_created: "2025-08-14T11:00:00Z",
                most_recently_updated: "2025-08-14T12:00:00Z"
              },
              computations: %{
                by_state: %{
                  success: 5000,
                  failed: 10,
                  computing: 5,
                  not_set: 2000,
                  abandoned: 50,
                  cancelled: 0
                },
                most_recently_created: "2025-08-14T12:00:00Z",
                most_recently_updated: "2025-08-14T12:00:00Z"
              }
            }
          }
        ]
      }

      expected_output = """
      System Status: HEALTHY
      Database: Connected
      ================================================================================

      GRAPHS (1 total):
      ----------
      Name: 'Test Graph #{test_id}'
      Version: 'v1.0.0'
      Executions:
      - active: 1.2k
      - archived: 567
      First activity: 2025-08-14T11:00:00Z
      Last activity: 2025-08-14T12:00:00Z
      Computations:
      ✓ success: 5.0k
      ✗ failed: 10
      ⏳ computing: 5
      ◯ not_set: 2.0k
      ⚠ abandoned: 50
      """

      assert Status.to_text(status_data) == expected_output
    end

    test "formats large numbers correctly", %{test_id: test_id} do
      status_data = %{
        status: :healthy,
        database_connected: true,
        graphs: [
          %{
            graph_name: "Large Graph #{test_id}",
            graph_version: "v1.0.0",
            stats: %{
              executions: %{
                active: 1_500_000,
                archived: 2_500_000,
                most_recently_created: "2025-08-14T10:00:00Z",
                most_recently_updated: "2025-08-14T12:00:00Z"
              },
              computations: %{
                by_state: %{
                  success: 10_000_000,
                  failed: 0,
                  computing: 0,
                  not_set: 0,
                  abandoned: 0,
                  cancelled: 0
                },
                most_recently_created: nil,
                most_recently_updated: nil
              }
            }
          }
        ]
      }

      expected_output = """
      System Status: HEALTHY
      Database: Connected
      ================================================================================

      GRAPHS (1 total):
      ----------
      Name: 'Large Graph #{test_id}'
      Version: 'v1.0.0'
      Executions:
      - active: 1.5M
      - archived: 2.5M
      First activity: 2025-08-14T10:00:00Z
      Last activity: 2025-08-14T12:00:00Z
      Computations:
      ✓ success: 10.0M
      """

      assert Status.to_text(status_data) == expected_output
    end

    test "sorts graphs by active execution count", %{test_id: test_id} do
      status_data = %{
        status: :healthy,
        database_connected: true,
        graphs: [
          %{
            graph_name: "Small Graph #{test_id}",
            graph_version: "v1.0.0",
            stats: %{
              executions: %{active: 10, archived: 0, most_recently_created: nil, most_recently_updated: nil},
              computations: %{by_state: %{}, most_recently_created: nil, most_recently_updated: nil}
            }
          },
          %{
            graph_name: "Large Graph #{test_id}",
            graph_version: "v1.0.0",
            stats: %{
              executions: %{active: 1000, archived: 0, most_recently_created: nil, most_recently_updated: nil},
              computations: %{by_state: %{}, most_recently_created: nil, most_recently_updated: nil}
            }
          },
          %{
            graph_name: "Medium Graph #{test_id}",
            graph_version: "v1.0.0",
            stats: %{
              executions: %{active: 100, archived: 0, most_recently_created: nil, most_recently_updated: nil},
              computations: %{by_state: %{}, most_recently_created: nil, most_recently_updated: nil}
            }
          }
        ]
      }

      text_output = Status.to_text(status_data)

      # Find positions of graph names in the output
      large_pos = :binary.match(text_output, "Large Graph") |> elem(0)
      medium_pos = :binary.match(text_output, "Medium Graph") |> elem(0)
      small_pos = :binary.match(text_output, "Small Graph") |> elem(0)

      # Verify ordering (largest first)
      assert large_pos < medium_pos
      assert medium_pos < small_pos
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
