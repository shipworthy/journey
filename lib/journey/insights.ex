defmodule Journey.Insights do
  @moduledoc """
  Provides system health and monitoring insights for Journey executions.
  """

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.ComputationState
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
end
