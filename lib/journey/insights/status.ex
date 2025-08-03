defmodule Journey.Insights.Status do
  @moduledoc """
  Provides system health and monitoring insights for Journey executions.
  """

  import Ecto.Query

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.ComputationState
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
    execution_stats = fetch_all_execution_stats()
    computation_stats = fetch_all_computation_stats()

    # Get all unique graph combinations from execution stats
    execution_stats
    |> Enum.map(fn {graph_name, graph_version, stats} ->
      comp_stats =
        Map.get(computation_stats, {graph_name, graph_version}, %{
          by_state: %{},
          most_recently_created: nil,
          most_recently_updated: nil
        })

      %{
        graph_name: graph_name,
        graph_version: graph_version,
        stats: %{
          executions: stats,
          computations: comp_stats
        }
      }
    end)
  end

  defp fetch_all_execution_stats() do
    from(e in Execution,
      group_by: [e.graph_name, e.graph_version],
      select: {
        e.graph_name,
        e.graph_version,
        %{
          archived: sum(fragment("CASE WHEN ? IS NOT NULL THEN 1 ELSE 0 END", e.archived_at)),
          active: sum(fragment("CASE WHEN ? IS NULL THEN 1 ELSE 0 END", e.archived_at)),
          most_recently_created: max(e.inserted_at),
          most_recently_updated: max(e.updated_at)
        }
      }
    )
    |> Repo.all()
    |> Enum.map(fn {graph_name, graph_version, stats} ->
      {graph_name, graph_version,
       %{
         archived: stats.archived || 0,
         active: stats.active || 0,
         most_recently_created: format_timestamp(stats.most_recently_created),
         most_recently_updated: format_timestamp(stats.most_recently_updated)
       }}
    end)
  end

  defp fetch_all_computation_stats() do
    # Single query to get all computation stats grouped by graph
    computation_data =
      from(c in Computation,
        join: e in Execution,
        on: c.execution_id == e.id,
        group_by: [e.graph_name, e.graph_version, c.state],
        select: {
          e.graph_name,
          e.graph_version,
          c.state,
          count(c.id)
        }
      )
      |> Repo.all()

    # Single query to get timestamps for all graphs
    timestamp_data =
      from(c in Computation,
        join: e in Execution,
        on: c.execution_id == e.id,
        group_by: [e.graph_name, e.graph_version],
        select: {
          e.graph_name,
          e.graph_version,
          max(c.inserted_at),
          max(c.updated_at)
        }
      )
      |> Repo.all()

    # Group computation counts by graph
    state_counts_by_graph =
      computation_data
      |> Enum.group_by(fn {graph_name, graph_version, _state, _count} ->
        {graph_name, graph_version}
      end)
      |> Enum.map(fn {{graph_name, graph_version}, state_data} ->
        # Initialize all states to 0
        by_state =
          ComputationState.values()
          |> Enum.map(&{&1, 0})
          |> Enum.into(%{})

        # Update with actual counts
        by_state =
          state_data
          |> Enum.reduce(by_state, fn {_graph_name, _graph_version, state, count}, acc ->
            Map.put(acc, state, count)
          end)

        {{graph_name, graph_version}, by_state}
      end)
      |> Enum.into(%{})

    # Group timestamps by graph
    timestamps_by_graph =
      timestamp_data
      |> Enum.map(fn {graph_name, graph_version, created, updated} ->
        {{graph_name, graph_version}, {created, updated}}
      end)
      |> Enum.into(%{})

    # Combine the data
    all_graphs =
      (Map.keys(state_counts_by_graph) ++ Map.keys(timestamps_by_graph))
      |> Enum.uniq()

    all_graphs
    |> Enum.map(fn {_graph_name, _graph_version} = key ->
      by_state = Map.get(state_counts_by_graph, key, %{})
      {created, updated} = Map.get(timestamps_by_graph, key, {nil, nil})

      {key,
       %{
         by_state: by_state,
         most_recently_created: format_timestamp(created),
         most_recently_updated: format_timestamp(updated)
       }}
    end)
    |> Enum.into(%{})
  end

  defp format_timestamp(nil), do: nil

  defp format_timestamp(timestamp) when is_integer(timestamp) do
    DateTime.from_unix!(timestamp)
    |> DateTime.to_iso8601()
  end
end
