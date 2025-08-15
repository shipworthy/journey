defmodule Journey.Insights.FlowAnalytics do
  @moduledoc """
  Provides system health and monitoring insights for Journey executions.
  """

  require Logger
  import Ecto.Query

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Repo

  @doc """
  Provides business-focused analytics for understanding customer behavior through Journey graphs.

  Uses optimized database queries that scale efficiently to millions of executions by leveraging
  PostgreSQL's aggregation capabilities. System nodes (execution_id and last_updated_at) are
  automatically excluded from the analysis.

  ## Parameters

  - `graph_name` - String, the graph name to analyze
  - `graph_version` - String, the graph version to analyze
  - `opts` - Keyword list with options:
    - `:include_executions` - `:all` | `:archived` | `:active` (default: `:active`)
    - `:flow_ends_here_after` - Duration in seconds after which we consider a flow "ended" if no activity (default: 86400 seconds / 1 day)


  ## Return Structure

  Returns a map with graph metadata, execution-level analytics, and per-node customer journey metrics.

  ## Examples

    iex(3)> Journey.Insights.FlowAnalytics.flow_analytics("Credit Card Application flow graph", "v1.0.0")
    %{
      graph_name: "Credit Card Application flow graph",
      analyzed_at: "2025-08-02T04:08:28.351195Z",
      executions: %{
        count: 8294,
        duration_avg_seconds_to_last_update: 48,
        duration_median_seconds_to_last_update: 0
      },
      graph_version: "v1.0.0",
      node_stats: %{
        nodes: [
          %{
            node_type: :input,
            node_name: :birth_date,
            # The number of executions that have set a value for this node.
            reached_count: 3884,
            # The average time it took for an execution to reach this node.
            average_time_to_reach: 1,
            # The number of executions which haven't been updated for a while, and this was the last node that was updated.
            flow_ends_here_count: 1953,
            # The percentage of all executions that ended here.
            flow_ends_here_percentage_of_all: 23.5,
            # The percentage of executions that reached this node and ended here.
            flow_ends_here_percentage_of_reached: 50.28,
            # The percentage of executions that have set a value for this node.
            reached_percentage: 46.8
          },
          %{
            node_type: :input,
            node_name: :email_address,
            reached_count: 2066,
            average_time_to_reach: 0,
            flow_ends_here_count: 213,
            flow_ends_here_percentage_of_all: 2.6,
            flow_ends_here_percentage_of_reached: 10.31,
            reached_percentage: 24.9
          },
          %{
            node_type: :input,
            node_name: :full_name,
            reached_count: 5736,
            average_time_to_reach: 0,
            flow_ends_here_count: 3716,
            flow_ends_here_percentage_of_all: 44.8,
            flow_ends_here_percentage_of_reached: 64.78,
            reached_percentage: 69.2
          },
          %{
            node_type: :compute,
            node_name: :credit_score,
            reached_count: 1844,
            average_time_to_reach: 1,
            flow_ends_here_count: 0,
            flow_ends_here_percentage_of_all: 0.0,
            flow_ends_here_percentage_of_reached: 0.0,
            reached_percentage: 22.2
          },
          ...
        ]
      }
    }
  """
  def flow_analytics(graph_name, graph_version, opts \\ []) do
    opts_schema = [
      include_executions: [is: {:in, [:active, :archived, :all]}],
      flow_ends_here_after: [is: :integer]
    ]

    KeywordValidator.validate!(opts, opts_schema)

    include_executions = Keyword.get(opts, :include_executions, :active)
    flow_ends_here_after = Keyword.get(opts, :flow_ends_here_after, 86_400)

    Logger.info(
      "flow_analytics: fetching flow analytics for graph #{graph_name} version #{graph_version}. include_executions: #{inspect(include_executions)}, considering flow ended after flow_ends_here_after: #{flow_ends_here_after} seconds"
    )

    try do
      # Query 1: Get execution-level analytics (count, median, average duration)
      execution_analytics = fetch_execution_analytics_optimized(graph_name, graph_version, include_executions)
      execution_count = execution_analytics.count

      # Query 2 & 3: Get all node analytics including flow ending logic
      node_stats =
        fetch_node_analytics_optimized(
          graph_name,
          graph_version,
          include_executions,
          flow_ends_here_after,
          execution_count
        )

      %{
        graph_name: graph_name,
        graph_version: graph_version,
        analyzed_at: DateTime.utc_now() |> DateTime.to_iso8601(),
        executions: execution_analytics,
        node_stats: %{
          nodes: node_stats
        }
      }
    rescue
      _e in DBConnection.ConnectionError ->
        %{
          graph_name: graph_name,
          graph_version: graph_version,
          analyzed_at: DateTime.utc_now() |> DateTime.to_iso8601(),
          error: "Database connection error",
          executions: %{count: 0, duration_median_seconds_to_last_update: 0, duration_avg_seconds_to_last_update: 0},
          node_stats: %{nodes: []}
        }
    end
  end

  @doc """
  Formats flow analytics data as human-readable text output.

  ## Example:

      iex> flow_data = Journey.Insights.FlowAnalytics.flow_analytics("Credit Card Application flow graph", "v1.0.0")
      iex> Journey.Insights.FlowAnalytics.to_text(flow_data) |> IO.puts()
      Graph: 'Credit Card Application flow graph'
      Version: 'v1.0.0'
      Analyzed at: 2025-08-02T04:08:28Z

      EXECUTION STATS:
      ----------
      Total executions: 8,294
      Average duration: 48 seconds
      Median duration: 0 seconds

      NODE STATS (4 nodes):
      ----------
      Node Name: 'birth_date'
      Type: input
      Reached by: 3,884 executions (46.8%)
      Average time to reach: 1 second
      Flow ends here: 1,953 executions (23.5% of all, 50.3% of reached)

      Node Name: 'email_address'
      Type: input
      Reached by: 2,066 executions (24.9%)
      Average time to reach: 0 seconds
      Flow ends here: 213 executions (2.6% of all, 10.3% of reached)

      Node Name: 'full_name'
      Type: input
      Reached by: 5,736 executions (69.2%)
      Average time to reach: 0 seconds
      Flow ends here: 3,716 executions (44.8% of all, 64.8% of reached)

      Node Name: 'credit_score'
      Type: compute
      Reached by: 1,844 executions (22.2%)
      Average time to reach: 1 second
      Flow ends here: 0 executions (0.0% of all, 0.0% of reached)
  """
  def to_text(flow_analytics) do
    graph_name = flow_analytics[:graph_name] || "Unknown"
    graph_version = flow_analytics[:graph_version] || "Unknown"
    analyzed_at = flow_analytics[:analyzed_at] || "Unknown"

    executions = flow_analytics[:executions] || %{}
    node_stats = flow_analytics[:node_stats] || %{}
    nodes = node_stats[:nodes] || []

    header = """
    Graph: '#{graph_name}'
    Version: '#{graph_version}'
    Analyzed at: #{analyzed_at}
    """

    execution_summary = format_execution_summary(executions)
    node_analytics = format_node_analytics(nodes)

    header <> execution_summary <> node_analytics
  end

  defp format_execution_summary(executions) do
    count = executions[:count] || 0
    avg_duration = executions[:duration_avg_seconds_to_last_update] || 0
    median_duration = executions[:duration_median_seconds_to_last_update] || 0

    """

    EXECUTION STATS:
    ----------
    Total executions: #{format_number(count)}
    Average duration: #{format_duration(avg_duration)}
    Median duration: #{format_duration(median_duration)}
    """
  end

  defp format_node_analytics(nodes) when nodes == [], do: "\nNODE STATS: No nodes found.\n"

  defp format_node_analytics(nodes) do
    node_count = length(nodes)

    nodes_text =
      nodes
      |> Enum.map_join("\n\n", &format_single_node/1)

    """

    NODE STATS (#{node_count} nodes):
    ----------
    #{nodes_text}
    """
  end

  defp format_single_node(node) do
    node_name = node[:node_name] || "unknown"
    node_type = node[:node_type] || "unknown"
    reached_count = node[:reached_count] || 0
    reached_percentage = node[:reached_percentage] || 0
    avg_time = node[:average_time_to_reach] || 0
    flow_ends_count = node[:flow_ends_here_count] || 0
    flow_ends_pct_all = node[:flow_ends_here_percentage_of_all] || 0
    flow_ends_pct_reached = node[:flow_ends_here_percentage_of_reached] || 0

    "Node Name: '#{node_name}'\nType: #{node_type}\nReached by: #{format_number(reached_count)} executions (#{format_percentage(reached_percentage)})\nAverage time to reach: #{format_duration(avg_time)}\nFlow ends here: #{format_number(flow_ends_count)} executions (#{format_percentage(flow_ends_pct_all)} of all, #{format_percentage(flow_ends_pct_reached)} of reached)"
  end

  defp format_number(num) when num >= 1_000_000 do
    "#{Float.round(num / 1_000_000, 1)}M"
  end

  defp format_number(num) when num >= 1_000 do
    "#{Float.round(num / 1_000, 1)}k"
  end

  defp format_number(num) when is_integer(num), do: Integer.to_string(num)
  defp format_number(num) when is_float(num), do: Float.to_string(num)
  defp format_number(_), do: "0"

  defp format_percentage(pct) when is_float(pct) do
    "#{Float.round(pct, 1)}%"
  end

  defp format_percentage(pct) when is_integer(pct) do
    "#{pct}.0%"
  end

  defp format_percentage(_), do: "0.0%"

  defp format_duration(0), do: "0 seconds"
  defp format_duration(1), do: "1 second"
  defp format_duration(seconds) when is_number(seconds), do: "#{round(seconds)} seconds"
  defp format_duration(_), do: "0 seconds"

  defp build_execution_filter_query(graph_name, graph_version, include_executions) do
    base_query =
      from(e in Execution,
        where: e.graph_name == ^graph_name and e.graph_version == ^graph_version,
        select: %{id: e.id, inserted_at: e.inserted_at}
      )

    case include_executions do
      :active -> from(e in base_query, where: is_nil(e.archived_at))
      :archived -> from(e in base_query, where: not is_nil(e.archived_at))
      :all -> base_query
    end
  end

  defp round_decimal(nil), do: 0
  defp round_decimal(decimal) when is_struct(decimal, Decimal), do: Decimal.to_integer(Decimal.round(decimal))
  defp round_decimal(number) when is_number(number), do: round(number)

  defp build_node_analytics(node, flow_ends_here_count, total_executions) do
    %{
      node_name: String.to_atom(node.node_name),
      node_type: node.node_type,
      reached_count: node.reached_count,
      reached_percentage: calculate_percentage(node.reached_count, total_executions, 1),
      average_time_to_reach: round_decimal(node.avg_time_to_reach || 0),
      flow_ends_here_count: flow_ends_here_count,
      flow_ends_here_percentage_of_all: calculate_percentage(flow_ends_here_count, total_executions, 1),
      flow_ends_here_percentage_of_reached: calculate_percentage(flow_ends_here_count, node.reached_count, 2)
    }
  end

  defp calculate_percentage(_numerator, 0, _precision), do: 0.0

  defp calculate_percentage(numerator, denominator, precision) do
    Float.round(numerator / denominator * 100, precision)
  end

  defp fetch_execution_analytics_optimized(graph_name, graph_version, include_executions) do
    # Single query to get execution count, median, and average duration
    base_query =
      from(e in Execution,
        where: e.graph_name == ^graph_name and e.graph_version == ^graph_version
      )

    query =
      case include_executions do
        :active -> from(e in base_query, where: is_nil(e.archived_at))
        :archived -> from(e in base_query, where: not is_nil(e.archived_at))
        :all -> base_query
      end

    result =
      from(e in query,
        select: %{
          count: count(e.id),
          duration_median_seconds:
            fragment("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (? - ?))", e.updated_at, e.inserted_at),
          duration_avg_seconds: avg(e.updated_at - e.inserted_at)
        }
      )
      |> Repo.one()

    if result.count == 0 do
      %{
        count: 0,
        duration_median_seconds_to_last_update: 0,
        duration_avg_seconds_to_last_update: 0
      }
    else
      %{
        count: result.count,
        duration_median_seconds_to_last_update: round_decimal(result.duration_median_seconds || 0),
        duration_avg_seconds_to_last_update: round_decimal(result.duration_avg_seconds || 0)
      }
    end
  end

  defp fetch_node_analytics_optimized(
         graph_name,
         graph_version,
         include_executions,
         flow_ends_here_after,
         total_executions
       ) do
    current_time = System.system_time(:second)
    flow_cutoff_time = current_time - flow_ends_here_after

    base_execution_filter = build_execution_filter_query(graph_name, graph_version, include_executions)

    # Get basic node statistics, excluding system nodes
    node_stats =
      from(v in Value,
        join: e in subquery(base_execution_filter),
        on: v.execution_id == e.id,
        where: not is_nil(v.set_time) and v.node_name not in ["execution_id", "last_updated_at"],
        group_by: [v.node_name, v.node_type],
        select: %{
          node_name: v.node_name,
          node_type: v.node_type,
          reached_count: count(v.execution_id, :distinct),
          avg_time_to_reach: avg(v.set_time - e.inserted_at)
        }
      )
      |> Repo.all()

    # For executions that haven't been updated in a while (flow_ends_here_after seconds), find the last updated node.
    flow_ending_counts =
      from(v in Value,
        join: e in subquery(base_execution_filter),
        on: v.execution_id == e.id,
        join:
          last_activity in subquery(
            from(v2 in Value,
              join: e2 in subquery(base_execution_filter),
              on: v2.execution_id == e2.id,
              where: not is_nil(v2.set_time) and v2.node_name not in ["execution_id", "last_updated_at"],
              group_by: v2.execution_id,
              select: %{execution_id: v2.execution_id, max_set_time: max(v2.set_time)}
            )
          ),
        on: v.execution_id == last_activity.execution_id,
        where:
          not is_nil(v.set_time) and
            v.set_time == last_activity.max_set_time and
            last_activity.max_set_time < ^flow_cutoff_time and
            v.node_name not in ["execution_id", "last_updated_at"],
        group_by: v.node_name,
        select: %{
          node_name: v.node_name,
          flow_ends_here_count: count(v.execution_id, :distinct)
        }
      )
      |> Repo.all()
      |> Enum.into(%{}, fn %{node_name: name, flow_ends_here_count: count} -> {name, count} end)

    # Combine results and calculate percentages (small dataset, done in memory)
    node_stats
    |> Enum.map(fn node ->
      flow_ends_here_count = Map.get(flow_ending_counts, node.node_name, 0)

      build_node_analytics(node, flow_ends_here_count, total_executions)
    end)
    |> Enum.sort_by(fn node -> {-node.reached_count, node.node_name} end)
  end
end
