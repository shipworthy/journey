defmodule Journey.InsightsTest do
  use ExUnit.Case, async: true

  alias Journey.Insights

  describe "status/0" do
    test "returns status response with expected structure" do
      result = Insights.status()

      assert %{
               status: status,
               database_connected: database_connected,
               graphs: graphs
             } = result

      assert status in [:healthy, :unhealthy]
      assert is_boolean(database_connected)
      assert is_list(graphs)
    end

    test "returns correct graph structure when graphs exist" do
      result = Insights.status()

      # If graphs exist, verify structure
      if length(result.graphs) > 0 do
        graph = hd(result.graphs)

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
                     by_state: state_counts,
                     most_recently_created: _,
                     most_recently_updated: _
                   }
                 }
               } = graph

        # Verify all computation states are included
        expected_states = [:not_set, :computing, :success, :failed, :abandoned, :cancelled]

        for state <- expected_states do
          assert Map.has_key?(state_counts, state)
          assert is_integer(state_counts[state])
        end
      end
    end

    test "timestamp format is correct when data exists" do
      result = Insights.status()

      if length(result.graphs) > 0 do
        graph = hd(result.graphs)
        exec_stats = graph.stats.executions

        # Test timestamp format if not nil
        if exec_stats.most_recently_created do
          assert exec_stats.most_recently_created =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/
          {:ok, _datetime, _offset} = DateTime.from_iso8601(exec_stats.most_recently_created)
        end

        if exec_stats.most_recently_updated do
          assert exec_stats.most_recently_updated =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/
          {:ok, _datetime, _offset} = DateTime.from_iso8601(exec_stats.most_recently_updated)
        end
      end
    end
  end
end
