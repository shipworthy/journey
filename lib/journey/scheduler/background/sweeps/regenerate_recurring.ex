defmodule Journey.Scheduler.Background.Sweeps.RegenerateScheduleRecurring do
  @moduledoc false

  require Logger
  import Ecto.Query

  import Journey.Helpers.Log
  import Journey.Scheduler.Background.Sweeps.Helpers
  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  # credo:disable-for-lines:10 Credo.Check.Refactor.CyclomaticComplexity
  @doc false
  def sweep(execution_id) when is_nil(execution_id) or is_binary(execution_id) do
    # Create new :not_set schedule_recurring records for schedule_recurring nodes
    # that have computed their scheduled time and whose scheduled time has passed.

    prefix = "[#{mf()}] [#{inspect(self())}]"
    Logger.info("#{prefix}: starting #{execution_id}")

    now = System.system_time(:second)

    # Get all registered graphs (same pattern as other sweepers)
    all_graphs =
      Journey.Graph.Catalog.list()
      |> Enum.map(fn g -> {g.name, g.version} end)

    regenerated_count =
      from(e in executions_for_graphs(execution_id, all_graphs),
        as: :main_execution,
        join: c in assoc(e, :computations),
        as: :main_computation,
        join: v in Value,
        on: v.execution_id == e.id and v.node_name == c.node_name,
        # No pending :not_set computation exists for this node
        where:
          is_nil(e.archived_at) and
            c.computation_type == ^:schedule_recurring and
            c.state == ^:success and
            v.node_type == ^:schedule_recurring and
            v.node_value <= ^now and
            not exists(
              from(c2 in Computation,
                where:
                  c2.execution_id == parent_as(:main_execution).id and
                    c2.node_name == parent_as(:main_computation).node_name and
                    c2.computation_type == ^:schedule_recurring and
                    c2.state == ^:not_set
              )
            ),
        distinct: [e.id, c.node_name],
        select: %{
          execution_id: e.id,
          node_name: c.node_name
        }
      )
      |> Journey.Repo.all()
      |> Enum.map(&create_new_recurring_computation/1)
      |> Enum.count()

    if regenerated_count == 0 do
      Logger.info("#{prefix}: no schedule_recurring nodes need regeneration")
    else
      Logger.info("#{prefix}: regenerated #{regenerated_count} schedule_recurring computation(s)")
    end
  end

  defp create_new_recurring_computation(%{
         execution_id: execution_id,
         node_name: node_name
       }) do
    # Create a new :not_set computation record for the next recurring cycle
    c =
      %Computation{
        execution_id: execution_id,
        node_name: node_name,
        computation_type: :schedule_recurring,
        state: :not_set,
        # These will be set when the computation actually runs
        ex_revision_at_start: nil,
        ex_revision_at_completion: nil,
        scheduled_time: nil,
        start_time: nil,
        completion_time: nil,
        deadline: nil,
        error_details: nil,
        computed_with: nil
      }
      |> Journey.Repo.insert!()

    prefix = "[#{execution_id}] [#{mf()}] [#{node_name}]"

    Logger.debug("#{prefix}: created a new :not_set computation, #{c.id}")
    c
  end
end
