defmodule Journey.Scheduler.Background.Sweeps.Helpers do
  @moduledoc false

  import Ecto.Query
  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Computation

  @doc false
  # construct a query to fetch computations, as long as they belong to one of the supplied graphs.
  def computations_for_graphs(nil, graph_names_and_versions) do
    # Build dynamic where clause for multiple graph name/version pairs
    conditions = filter_by_graphs(graph_names_and_versions)
    from(c in Computation, join: e in Execution, on: c.execution_id == e.id, where: ^conditions)
  end

  @doc false
  def computations_for_graphs(execution_id, graph_names_and_versions) do
    # Build dynamic where clause for multiple graph name/version pairs
    graph_conditions = filter_by_graphs(graph_names_and_versions)

    # Combine execution_id filter with graph filters using dynamic
    conditions = dynamic([c, e], e.id == ^execution_id and ^graph_conditions)

    from(c in Computation,
      join: e in Execution,
      on: c.execution_id == e.id,
      where: ^conditions
    )
  end

  defp filter_by_graphs(graph_names_and_versions) do
    graph_names_and_versions
    |> Enum.reduce(dynamic(false), fn {name, version}, acc ->
      dynamic([c, e], ^acc or (e.graph_name == ^name and e.graph_version == ^version))
    end)
  end
end
