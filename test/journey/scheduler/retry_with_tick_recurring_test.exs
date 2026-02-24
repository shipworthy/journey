defmodule Journey.Scheduler.RetryWithTickRecurringTest do
  use ExUnit.Case, async: true

  require Logger

  import Ecto.Query
  import Journey.Test.Support.Helpers

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.Execution.Value

  @moduletag timeout: 60_000

  # Reproduces the bug where a compute node gated by tick_recurring permanently
  # exhausts max_retries because get_max_upstream_revision used provided? to
  # determine which upstream revisions to include. When the schedule node has
  # already advanced to the next cycle (node_value in the future), provided?
  # returns false, causing the retry counter to scope against only the non-schedule
  # upstream revision and count ALL historical failures — not just the current cycle.
  #
  # Setup:
  #   1. Create a graph with tick_recurring -> compute (max_retries: 3)
  #   2. Let cycle 1 complete naturally (tick fires, compute succeeds)
  #   3. Simulate the post-regeneration state: schedule has a far-future node_value
  #      (provided? returns false) but set_time is non-nil
  #   4. Insert failed computations: 2 from old cycles + 1 from current cycle
  #   5. Call maybe_schedule_a_retry for the current cycle's failure
  #
  # With the fix: upstream revision includes the schedule node's revision (via
  #   list_all_node_names + set_time check) → only 1 recent attempt → retry created
  #
  # Without the fix: provided? returns false for the schedule node → upstream
  #   revision falls back to only :enabled's low revision → all 3 failures counted
  #   → 3 >= max_retries (3) → no retry
  test "retry counter scopes to current cycle when schedule node has future value" do
    graph_name = "retry_tick_recurring_test_#{Journey.Helpers.Random.random_string()}"

    graph =
      Journey.new_graph(
        graph_name,
        "1.0.0",
        [
          input(:enabled),
          tick_recurring(
            :schedule,
            unblocked_when({:and, [{:enabled, &true?/1}]}),
            fn _values ->
              {:ok, System.system_time(:second) + 2}
            end
          ),
          compute(
            :do_work,
            unblocked_when({:and, [{:enabled, &true?/1}, {:schedule, &provided?/1}]}),
            fn _values ->
              {:ok, 1}
            end,
            max_retries: 3
          )
        ]
      )

    execution = Journey.start_execution(graph)
    background_sweeps_task = start_background_sweeps_in_test(execution.id)
    execution = Journey.set(execution, :enabled, true)

    # Wait for cycle 1 to complete: tick fires, do_work succeeds
    assert wait_for_value(execution, :do_work, 1, timeout: 15_000)

    stop_background_sweeps_in_test(background_sweeps_task)

    # Now simulate the bug scenario: schedule has regenerated with a far-future
    # node_value (provided? returns false), and there are old failed computations.

    # Use a high revision to represent a new cycle's upstream state
    new_cycle_revision = 1000
    now = System.system_time(:second)

    # Update :schedule value: far-future node_value (provided? will return false),
    # but set_time is non-nil (the fix checks set_time, not provided?)
    from(v in Value,
      where: v.execution_id == ^execution.id and v.node_name == "schedule"
    )
    |> Journey.Repo.update_all(set: [node_value: now + 100_000, ex_revision: new_cycle_revision, set_time: now])

    # :enabled keeps a LOW revision (it was set once early and never changed).
    # This is the crux of the bug: without the fix, max_upstream_revision = enabled's
    # low revision → all historical computations are "recent"
    enabled_revision = 2

    from(v in Value,
      where: v.execution_id == ^execution.id and v.node_name == "enabled"
    )
    |> Journey.Repo.update_all(set: [ex_revision: enabled_revision])

    # Insert 2 failed computations from "old" cycles (low revision)
    for _ <- 1..2 do
      %Computation{
        id: Journey.Helpers.Random.object_id("CMP"),
        execution_id: execution.id,
        node_name: "do_work",
        computation_type: :compute,
        state: :failed,
        ex_revision_at_start: enabled_revision + 1
      }
      |> Journey.Repo.insert!()
    end

    # Insert 1 failed computation from the "current" cycle (high revision, matching schedule)
    current_failed =
      %Computation{
        id: Journey.Helpers.Random.object_id("CMP"),
        execution_id: execution.id,
        node_name: "do_work",
        computation_type: :compute,
        state: :failed,
        ex_revision_at_start: new_cycle_revision
      }
      |> Journey.Repo.insert!()

    # Count :not_set computations before calling retry
    before_count =
      from(c in Computation,
        where:
          c.execution_id == ^execution.id and
            c.node_name == "do_work" and
            c.state == :not_set,
        select: count()
      )
      |> Journey.Repo.one()

    # Call maybe_schedule_a_retry for the current cycle's failed computation.
    # The function expects node_name as an atom.
    current_failed_atom = %{current_failed | node_name: :do_work}
    Journey.Scheduler.Retry.maybe_schedule_a_retry(current_failed_atom, Journey.Repo)

    # Count :not_set computations after
    after_count =
      from(c in Computation,
        where:
          c.execution_id == ^execution.id and
            c.node_name == "do_work" and
            c.state == :not_set,
        select: count()
      )
      |> Journey.Repo.one()

    # With fix: max_upstream_revision = new_cycle_revision (1000)
    #   → only 1 computation has ex_revision_at_start >= 1000
    #   → 1 < max_retries (3) → retry created
    #
    # Without fix: provided? false for :schedule → max_upstream_revision = enabled_revision (2)
    #   → 3 computations have ex_revision_at_start >= 2 (2 old + 1 current)
    #   → 3 >= max_retries (3) → NO retry
    assert after_count == before_count + 1,
           "Expected a retry computation to be created. " <>
             "The retry counter should scope to the current cycle's upstream revision, " <>
             "not count all historical failures. before=#{before_count}, after=#{after_count}"
  end
end
