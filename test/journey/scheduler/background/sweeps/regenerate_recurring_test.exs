defmodule Journey.Scheduler.Background.Sweeps.RegenerateScheduleRecurringTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  import Journey.Node
  import Journey.Helpers.Random

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Scheduler.Background.Sweeps.RegenerateScheduleRecurring

  describe "sweep/1" do
    test "updates execution.updated_at when creating a new :not_set computation" do
      # RegenerateScheduleRecurring creates :not_set computations for the next
      # tick_recurring cycle. ScheduleNodes filters on e.updated_at >= cutoff_time,
      # so if updated_at isn't refreshed, ScheduleNodes can't find the new computation.

      graph =
        Journey.new_graph(
          "regenerate-test-#{random_string()}",
          "1.0",
          [
            tick_recurring(:my_tick, [], fn _ -> {:ok, System.system_time(:second) + 300} end)
          ]
        )

      execution = Journey.start_execution(graph)

      now = System.system_time(:second)
      old_time = now - 600

      # Set computation to :success with a past node_value (pulse already due)
      from(c in Computation,
        where: c.execution_id == ^execution.id and c.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [state: :success, completion_time: old_time])

      from(v in Value,
        where: v.execution_id == ^execution.id and v.node_name == "my_tick"
      )
      |> Journey.Repo.update_all(set: [node_value: now - 100, set_time: old_time, ex_revision: 1])

      # Set execution.updated_at to an old time
      from(e in Execution, where: e.id == ^execution.id)
      |> Journey.Repo.update_all(set: [updated_at: old_time])

      # Verify updated_at is old
      exec_before = Journey.Repo.get!(Execution, execution.id)
      assert exec_before.updated_at == old_time

      # Run the sweep — should create a :not_set and refresh updated_at
      RegenerateScheduleRecurring.sweep(execution.id)

      # Verify updated_at was refreshed
      exec_after = Journey.Repo.get!(Execution, execution.id)
      assert exec_after.updated_at >= now
    end
  end
end
