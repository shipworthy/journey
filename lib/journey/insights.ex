defmodule Journey.Insights do
  @moduledoc """
  Provides system health and monitoring insights for Journey executions.
  """

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.ComputationState
  alias Journey.Execution.Value
  alias Journey.Repo

  @doc """
  Returns current system health for monitoring/alerting

  ## Response Structure

  - `status` - `:healthy` or `:unhealthy`
  - `database_connected` - Boolean indicating DB connectivity
  - `graphs` - List of graph statistics, one per graph name/version

  ## Example output:

  ```elixir
  %{
    status: :healthy,
    graphs: [
      %{
        stats: %{
          computations: %{
            by_state: %{
              abandoned: 239,
              cancelled: 0,
              success: 21106,
              failed: 0,
              not_set: 59294,
              computing: 0
            },
            most_recently_created: "2025-07-30T00:07:37Z",
            most_recently_updated: "2025-07-30T00:07:41Z"
          },
          executions: %{
            active: 4597,
            most_recently_created: "2025-07-30T00:07:37Z",
            most_recently_updated: "2025-07-30T00:07:41Z",
            archived: 2103
          }
        },
        graph_name: "Credit Card Application flow graph",
        graph_version: "v1.0.0"
      }
    ],
    database_connected: true
  }
  ```
  """
  def status() do
    graphs_data = fetch_graphs_data()

    %{
      status: :healthy,
      database_connected: true,
      graphs: graphs_data
    }
  rescue
    _e in DBConnection.ConnectionError ->
      %{
        status: :unhealthy,
        database_connected: false,
        graphs: []
      }

    _e ->
      %{
        status: :unhealthy,
        database_connected: true,
        graphs: []
      }
  end

  defp fetch_graphs_data() do
    # Get unique graph name/version combinations
    graph_combinations =
      from(e in Execution,
        group_by: [e.graph_name, e.graph_version],
        select: {e.graph_name, e.graph_version}
      )
      |> Repo.all()

    # Fetch stats for each combination
    Enum.map(graph_combinations, fn {graph_name, graph_version} ->
      %{
        graph_name: graph_name,
        graph_version: graph_version,
        stats: %{
          executions: fetch_execution_stats(graph_name, graph_version),
          computations: fetch_computation_stats(graph_name, graph_version)
        }
      }
    end)
  end

  defp fetch_execution_stats(graph_name, graph_version) do
    # Count archived executions
    archived_count =
      from(e in Execution,
        where:
          e.graph_name == ^graph_name and
            e.graph_version == ^graph_version and
            not is_nil(e.archived_at),
        select: count(e.id)
      )
      |> Repo.one()

    # Count active executions
    active_count =
      from(e in Execution,
        where:
          e.graph_name == ^graph_name and
            e.graph_version == ^graph_version and
            is_nil(e.archived_at),
        select: count(e.id)
      )
      |> Repo.one()

    # Get most recently created timestamp
    most_recently_created =
      from(e in Execution,
        where:
          e.graph_name == ^graph_name and
            e.graph_version == ^graph_version,
        select: max(e.inserted_at)
      )
      |> Repo.one()
      |> format_timestamp()

    # Get most recently updated timestamp
    most_recently_updated =
      from(e in Execution,
        where:
          e.graph_name == ^graph_name and
            e.graph_version == ^graph_version,
        select: max(e.updated_at)
      )
      |> Repo.one()
      |> format_timestamp()

    %{
      archived: archived_count,
      active: active_count,
      most_recently_created: most_recently_created,
      most_recently_updated: most_recently_updated
    }
  end

  defp fetch_computation_stats(graph_name, graph_version) do
    # Use a single JOIN query to get computation stats
    # This avoids loading all execution IDs into memory
    state_counts =
      ComputationState.values()
      |> Enum.map(fn state ->
        count =
          from(c in Computation,
            join: e in Execution,
            on: c.execution_id == e.id,
            where:
              e.graph_name == ^graph_name and
                e.graph_version == ^graph_version and
                c.state == ^state,
            select: count(c.id)
          )
          |> Repo.one()

        {state, count}
      end)
      |> Enum.into(%{})

    # Get most recently created computation timestamp
    most_recently_created =
      from(c in Computation,
        join: e in Execution,
        on: c.execution_id == e.id,
        where:
          e.graph_name == ^graph_name and
            e.graph_version == ^graph_version,
        select: max(c.inserted_at)
      )
      |> Repo.one()
      |> format_timestamp()

    # Get most recently updated computation timestamp
    most_recently_updated =
      from(c in Computation,
        join: e in Execution,
        on: c.execution_id == e.id,
        where:
          e.graph_name == ^graph_name and
            e.graph_version == ^graph_version,
        select: max(c.updated_at)
      )
      |> Repo.one()
      |> format_timestamp()

    %{
      by_state: state_counts,
      most_recently_created: most_recently_created,
      most_recently_updated: most_recently_updated
    }
  end

  defp format_timestamp(nil), do: nil

  defp format_timestamp(timestamp) when is_integer(timestamp) do
    DateTime.from_unix!(timestamp)
    |> DateTime.to_iso8601()
  end

  @doc """
  Provides business-focused analytics for understanding customer behavior through Journey graphs.

  ## Parameters

  - `graph_name` - String, the graph name to analyze
  - `graph_version` - String, the graph version to analyze
  - `opts` - Keyword list with options:
    - `:include_executions` - `:all` | `:archived` | `:active` (default: `:active`)
    - `:flow_ends_here_after` - Duration in seconds after which we consider a flow "ended" if no activity (default: 86400 seconds / 1 day)

  ## Return Structure

  Returns a map with graph metadata, execution-level analytics, and per-node customer journey metrics.

  ## Examples

      iex> Journey.Insights.flow_analytics("my_graph", "v1.0")
      %{
        graph_name: "my_graph",
        graph_version: "v1.0",
        analyzed_at: "2025-07-31T10:30:00Z",
        executions: %{
          count: 100,
          duration_median_seconds_to_last_update: 300,
          duration_avg_seconds_to_last_update: 450
        },
        node_stats: %{
          nodes: [
            %{
              node_name: :input_node,
              node_type: :input,
              reached_count: 95,
              reached_percentage: 95.0,
              average_time_to_reach: 10,
              flow_ends_here_count: 5,
              flow_ends_here_percentage_of_all: 5.0,
              flow_ends_here_percentage_of_reached: 5.26
            }
          ]
        }
      }
  """
  def flow_analytics(graph_name, graph_version, opts \\ []) do
    include_executions = Keyword.get(opts, :include_executions, :active)
    flow_ends_here_after = Keyword.get(opts, :flow_ends_here_after, 86_400)

    try do
      # Get execution data
      executions_data = fetch_executions_for_analytics(graph_name, graph_version, include_executions)
      execution_count = length(executions_data)

      # Calculate execution-level analytics
      execution_analytics = calculate_execution_analytics(executions_data)

      # Get node statistics
      node_stats =
        fetch_node_analytics(graph_name, graph_version, include_executions, flow_ends_here_after, execution_count)

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

  defp fetch_executions_for_analytics(graph_name, graph_version, include_executions) do
    base_query =
      from(e in Execution,
        where: e.graph_name == ^graph_name and e.graph_version == ^graph_version,
        select: %{
          id: e.id,
          inserted_at: e.inserted_at,
          updated_at: e.updated_at,
          archived_at: e.archived_at
        }
      )

    query =
      case include_executions do
        :active -> from(e in base_query, where: is_nil(e.archived_at))
        :archived -> from(e in base_query, where: not is_nil(e.archived_at))
        :all -> base_query
      end

    Repo.all(query)
  end

  defp calculate_execution_analytics(executions_data) do
    count = length(executions_data)

    if count == 0 do
      %{
        count: 0,
        duration_median_seconds_to_last_update: 0,
        duration_avg_seconds_to_last_update: 0
      }
    else
      durations =
        Enum.map(executions_data, fn exec ->
          exec.updated_at - exec.inserted_at
        end)

      sorted_durations = Enum.sort(durations)
      median = calculate_median(sorted_durations)
      average = Enum.sum(durations) / count

      %{
        count: count,
        duration_median_seconds_to_last_update: round(median),
        duration_avg_seconds_to_last_update: round(average)
      }
    end
  end

  defp calculate_median(sorted_list) do
    len = length(sorted_list)

    if len == 0 do
      0
    else
      mid = div(len, 2)

      if rem(len, 2) == 1 do
        Enum.at(sorted_list, mid)
      else
        (Enum.at(sorted_list, mid - 1) + Enum.at(sorted_list, mid)) / 2
      end
    end
  end

  defp fetch_node_analytics(graph_name, graph_version, include_executions, flow_ends_here_after, total_executions) do
    # Get all unique nodes for this graph/version combination
    base_execution_query = build_execution_filter_query(graph_name, graph_version, include_executions)

    # This was used for debugging - removing unused variable

    # Get node statistics with reach counts and timing data
    # Only count nodes where a value was actually set (set_time is not null)
    node_query =
      from(v in Value,
        join: e in subquery(base_execution_query),
        on: v.execution_id == e.id,
        where: not is_nil(v.set_time),
        group_by: [v.node_name, v.node_type],
        select: %{
          node_name: v.node_name,
          node_type: v.node_type,
          reached_count: count(v.execution_id, :distinct),
          avg_time_to_reach: avg(v.set_time - e.inserted_at),
          min_set_time: min(v.set_time),
          max_set_time: max(v.set_time)
        }
      )

    node_data = Repo.all(node_query)

    # Calculate flow ending statistics for each node
    current_time = System.system_time(:second)
    flow_cutoff_time = current_time - flow_ends_here_after

    Enum.map(node_data, fn node ->
      # Count executions where this node was the last activity and flow ended
      flow_ends_query =
        from(v in Value,
          join: e in subquery(base_execution_query),
          on: v.execution_id == e.id,
          join:
            last_v in subquery(
              from(v2 in Value,
                join: e2 in subquery(base_execution_query),
                on: v2.execution_id == e2.id,
                where: not is_nil(v2.set_time),
                group_by: v2.execution_id,
                select: %{execution_id: v2.execution_id, max_set_time: max(v2.set_time)}
              )
            ),
          on: v.execution_id == last_v.execution_id,
          where:
            v.node_name == ^node.node_name and
              not is_nil(v.set_time) and
              v.set_time == last_v.max_set_time and
              last_v.max_set_time < ^flow_cutoff_time,
          select: count(v.execution_id, :distinct)
        )

      flow_ends_here_count = Repo.one(flow_ends_query) || 0

      reached_percentage = if total_executions > 0, do: node.reached_count / total_executions * 100, else: 0.0

      flow_ends_here_percentage_of_all =
        if total_executions > 0, do: flow_ends_here_count / total_executions * 100, else: 0.0

      flow_ends_here_percentage_of_reached =
        if node.reached_count > 0, do: flow_ends_here_count / node.reached_count * 100, else: 0.0

      %{
        node_name: String.to_atom(node.node_name),
        node_type: node.node_type,
        reached_count: node.reached_count,
        reached_percentage: Float.round(reached_percentage, 1),
        average_time_to_reach: round_decimal(node.avg_time_to_reach || 0),
        flow_ends_here_count: flow_ends_here_count,
        flow_ends_here_percentage_of_all: Float.round(flow_ends_here_percentage_of_all, 1),
        flow_ends_here_percentage_of_reached: Float.round(flow_ends_here_percentage_of_reached, 2)
      }
    end)
  end

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
end
