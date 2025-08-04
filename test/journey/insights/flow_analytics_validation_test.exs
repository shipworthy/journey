defmodule Journey.Insights.FlowAnalyticsValidationTest do
  use ExUnit.Case, async: true

  alias Journey.Insights.FlowAnalytics, as: Insights

  describe "flow_analytics/3 validation" do
    test "validates include_executions option" do
      # Valid values should work
      assert {:ok, _} = safe_flow_analytics("test_graph", "1.0.0", include_executions: :active)
      assert {:ok, _} = safe_flow_analytics("test_graph", "1.0.0", include_executions: :archived)
      assert {:ok, _} = safe_flow_analytics("test_graph", "1.0.0", include_executions: :all)

      # Invalid values should raise
      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0", include_executions: :invalid_value)
      end

      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0", include_executions: "active")
      end

      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0", include_executions: nil)
      end
    end

    test "validates flow_ends_here_after option" do
      # Valid integer values should work
      assert {:ok, _} = safe_flow_analytics("test_graph", "1.0.0", flow_ends_here_after: 0)
      assert {:ok, _} = safe_flow_analytics("test_graph", "1.0.0", flow_ends_here_after: 3600)
      assert {:ok, _} = safe_flow_analytics("test_graph", "1.0.0", flow_ends_here_after: 86_400)

      # Invalid types should raise
      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0", flow_ends_here_after: "3600")
      end

      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0", flow_ends_here_after: 3.14)
      end

      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0", flow_ends_here_after: :invalid)
      end
    end

    test "rejects unknown options" do
      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0", unknown_option: :value)
      end

      assert_raise ArgumentError, fn ->
        Insights.flow_analytics("test_graph", "1.0.0",
          include_executions: :active,
          unknown_option: :value,
          flow_ends_here_after: 3600
        )
      end
    end

    test "allows empty options" do
      assert {:ok, result} = safe_flow_analytics("test_graph", "1.0.0", [])
      assert result.graph_name == "test_graph"
      assert result.graph_version == "1.0.0"
    end

    test "allows valid combination of options" do
      assert {:ok, result} =
               safe_flow_analytics("test_graph", "1.0.0",
                 include_executions: :all,
                 flow_ends_here_after: 7200
               )

      assert result.graph_name == "test_graph"
      assert result.graph_version == "1.0.0"
    end
  end

  # Helper functions
  defp safe_flow_analytics(graph_name, graph_version, opts) do
    {:ok, Insights.flow_analytics(graph_name, graph_version, opts)}
  rescue
    e in ArgumentError -> {:error, e}
  end
end
