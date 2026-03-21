defmodule Journey.Scheduler.RecomputeStopsAfterMaxRetriesTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import Journey.Node

  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  # Reproduces the bug where Recompute.detect_updates_and_create_re_computations
  # kept creating new computations for a node that had permanently failed after
  # exhausting max_retries. The NOT EXISTS check in atomic_insert_if_no_duplicate
  # only checked for :not_set, :computing, and newer :success states — never
  # :failed — so after retry gave up, recompute would create a fresh computation
  # on every sweep, causing an infinite loop.
  #
  # Setup mirrors retry_with_tick_recurring_test.exs: let the graph run naturally
  # for one cycle, then directly manipulate DB state to simulate the failure scenario.
  test "recompute does not create new computations for permanently failed nodes" do
    graph_name = "recompute_max_retries_#{Journey.Helpers.Random.random_string()}"

    graph =
      Journey.new_graph(
        graph_name,
        "1.0.0",
        [
          input(:trigger),
          compute(
            :downstream,
            [:trigger],
            fn _values -> {:ok, "computed"} end,
            max_retries: 3
          )
        ]
      )

    execution = Journey.start_execution(graph)

    # Let the graph run naturally: set trigger, wait for compute to succeed
    execution = Journey.set(execution, :trigger, "v1")
    {:ok, "computed", _rev} = Journey.get(execution, :downstream, wait: :any)

    # Now simulate: upstream changed (trigger revision bumped) but all compute
    # attempts at the new revision have failed.
    new_upstream_rev = 1000

    from(v in Value,
      where: v.execution_id == ^execution.id and v.node_name == "trigger"
    )
    |> Journey.Repo.update_all(set: [ex_revision: new_upstream_rev])

    # Insert 3 failed computations at the new upstream revision
    for _ <- 1..3 do
      %Computation{
        id: Journey.Helpers.Random.object_id("CMP"),
        execution_id: execution.id,
        node_name: "downstream",
        computation_type: :compute,
        state: :failed,
        ex_revision_at_start: new_upstream_rev
      }
      |> Journey.Repo.insert!()
    end

    before_count = count_not_set_computations(execution.id, "downstream")

    # Trigger recompute — should NOT create a new computation
    execution = Journey.load(execution)
    Journey.Scheduler.Recompute.detect_updates_and_create_re_computations(execution, graph)

    after_count = count_not_set_computations(execution.id, "downstream")

    assert after_count == before_count,
           "Expected no new computation after max retries exhausted. " <>
             "before=#{before_count}, after=#{after_count}"
  end

  # Verifies that when upstream genuinely changes AFTER failures, recompute
  # correctly creates a new computation. Old failures have ex_revision_at_start
  # below the new max upstream revision, so they no longer block.
  test "recompute creates new computation after upstream changes for previously failed node" do
    graph_name = "recompute_upstream_change_#{Journey.Helpers.Random.random_string()}"

    graph =
      Journey.new_graph(
        graph_name,
        "1.0.0",
        [
          input(:trigger),
          compute(
            :downstream,
            [:trigger],
            fn _values -> {:ok, "computed"} end,
            max_retries: 3
          )
        ]
      )

    execution = Journey.start_execution(graph)

    # Let the graph run naturally
    execution = Journey.set(execution, :trigger, "v1")
    {:ok, "computed", _rev} = Journey.get(execution, :downstream, wait: :any)

    # Simulate: upstream changed to revision 1000, compute failed 3 times
    old_upstream_rev = 1000

    from(v in Value,
      where: v.execution_id == ^execution.id and v.node_name == "trigger"
    )
    |> Journey.Repo.update_all(set: [ex_revision: old_upstream_rev])

    for _ <- 1..3 do
      %Computation{
        id: Journey.Helpers.Random.object_id("CMP"),
        execution_id: execution.id,
        node_name: "downstream",
        computation_type: :compute,
        state: :failed,
        ex_revision_at_start: old_upstream_rev
      }
      |> Journey.Repo.insert!()
    end

    # Now upstream changes AGAIN to a higher revision (simulating a new input update)
    new_upstream_rev = 2000

    from(v in Value,
      where: v.execution_id == ^execution.id and v.node_name == "trigger"
    )
    |> Journey.Repo.update_all(set: [ex_revision: new_upstream_rev])

    before_count = count_not_set_computations(execution.id, "downstream")

    # Trigger recompute — SHOULD create a new computation (upstream changed past failures)
    execution = Journey.load(execution)
    Journey.Scheduler.Recompute.detect_updates_and_create_re_computations(execution, graph)

    after_count = count_not_set_computations(execution.id, "downstream")

    assert after_count == before_count + 1,
           "Expected a new computation after upstream changed past previous failures. " <>
             "before=#{before_count}, after=#{after_count}"
  end

  defp count_not_set_computations(execution_id, node_name) do
    from(c in Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^node_name and
          c.state == :not_set,
      select: count()
    )
    |> Journey.Repo.one()
  end
end
