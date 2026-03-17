defmodule Journey.Scheduler.RetentionTest do
  use ExUnit.Case, async: true

  require Logger

  import Ecto.Query
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  alias Journey.Helpers.Random
  alias Journey.Persistence.Schema.Execution.Computation

  defp count_successful_computations(execution_id, node_name) do
    from(c in Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^Atom.to_string(node_name) and
          c.state == :success
    )
    |> Journey.Repo.aggregate(:count)
  end

  defp successful_computation_revisions(execution_id, node_name) do
    from(c in Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^Atom.to_string(node_name) and
          c.state == :success,
      order_by: [asc: c.ex_revision_at_completion],
      select: c.ex_revision_at_completion
    )
    |> Journey.Repo.all()
  end

  # Waits until the counter reaches the target count, then allows extra time
  # for the last completion to commit to the database.
  defp wait_for_cycles(counter_ref, target, timeout_ms \\ 30_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    result = do_wait_for_cycles(counter_ref, target, deadline)

    if result do
      # Allow time for the last computation to commit to the DB
      Process.sleep(500)
    end

    result
  end

  defp do_wait_for_cycles(counter_ref, target, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      current = :counters.get(counter_ref, 1)

      Logger.warning("Timed out waiting for #{target} cycles, currently at #{current}")

      false
    else
      if :counters.get(counter_ref, 1) >= target do
        true
      else
        Process.sleep(200)
        do_wait_for_cycles(counter_ref, target, deadline)
      end
    end
  end

  defp make_tick_fn(counter_ref, interval_seconds \\ 2) do
    fn _ ->
      :counters.add(counter_ref, 1, 1)
      {:ok, System.system_time(:second) + interval_seconds}
    end
  end

  # -- Validation tests --

  test "rejects negative keep_latest_completed_computations on node" do
    assert_raise ArgumentError, ~r/keep_latest_completed_computations/, fn ->
      compute(:foo, [], fn _ -> {:ok, 1} end, keep_latest_completed_computations: -5)
    end
  end

  test "rejects zero keep_oldest_completed_computations on node" do
    assert_raise ArgumentError, ~r/keep_oldest_completed_computations/, fn ->
      compute(:foo, [], fn _ -> {:ok, 1} end, keep_oldest_completed_computations: 0)
    end
  end

  test "rejects invalid keep_latest_completed_computations on graph" do
    assert_raise ArgumentError, ~r/keep_latest_completed_computations/, fn ->
      Journey.new_graph(
        "retention_test reject_invalid_graph #{Random.random_string()}",
        "v1",
        [input(:x)],
        keep_latest_completed_computations: -1
      )
    end
  end

  test "accepts valid retention options on node" do
    step =
      compute(:foo, [], fn _ -> {:ok, 1} end,
        keep_latest_completed_computations: 50,
        keep_oldest_completed_computations: 5
      )

    assert step.keep_latest_completed_computations == 50
    assert step.keep_oldest_completed_computations == 5
  end

  test "accepts :all on node" do
    step = compute(:foo, [], fn _ -> {:ok, 1} end, keep_latest_completed_computations: :all)
    assert step.keep_latest_completed_computations == :all
  end

  test "defaults to nil when not set" do
    step = compute(:foo, [], fn _ -> {:ok, 1} end)
    assert step.keep_latest_completed_computations == nil
    assert step.keep_oldest_completed_computations == nil
  end

  # -- Resolution: node integer overrides graph --

  @tag timeout: 60_000
  test "node integer overrides graph-level keep_latest" do
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test node_overrides_graph #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter),
            keep_latest_completed_computations: 2
          )
        ],
        keep_latest_completed_computations: 100
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    # Wait for 15+ cycles (enough for cleanup to have triggered multiple times)
    assert wait_for_cycles(counter, 15)

    # Node says keep_latest: 2, default keep_oldest: 10, so max is 12
    success_count = count_successful_computations(execution.id, :schedule)
    assert success_count <= 12

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Resolution: node :all overrides graph integer --

  @tag timeout: 60_000
  test "node :all overrides graph integer (keeps everything)" do
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test node_all_overrides_graph #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter),
            keep_latest_completed_computations: :all
          )
        ],
        keep_latest_completed_computations: 2
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    assert wait_for_cycles(counter, 8)

    # Node says :all, so ALL should be kept despite graph saying 2
    success_count = count_successful_computations(execution.id, :schedule)
    assert success_count >= 8

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Resolution: node nil falls through to graph --

  @tag timeout: 90_000
  test "node nil falls through to graph integer" do
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test node_nil_inherits_graph #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter)
          )
        ],
        keep_latest_completed_computations: 3
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    assert wait_for_cycles(counter, 18, 60_000)

    # Graph says 3, default keep_oldest is 10, so max is 13
    success_count = count_successful_computations(execution.id, :schedule)
    assert success_count <= 13

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Default: no cleanup --

  @tag timeout: 60_000
  test "no cleanup when neither graph nor node sets retention" do
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test no_cleanup_default #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter)
          )
        ]
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    assert wait_for_cycles(counter, 8)

    # All computations should be preserved
    success_count = count_successful_computations(execution.id, :schedule)
    assert success_count >= 8

    stop_background_sweeps_in_test(sweeps)
  end

  @tag timeout: 60_000
  test "graph-level :all explicitly keeps everything" do
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test graph_all_keeps_everything #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter)
          )
        ],
        keep_latest_completed_computations: :all
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    assert wait_for_cycles(counter, 8)

    success_count = count_successful_computations(execution.id, :schedule)
    assert success_count >= 8

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Retention bounds computation count --

  @tag timeout: 60_000
  test "retention bounds computation count for tick_recurring" do
    keep_latest = 3
    keep_oldest = 2
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test bounds_count #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter),
            keep_latest_completed_computations: keep_latest,
            keep_oldest_completed_computations: keep_oldest
          )
        ]
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    # Wait for enough cycles that cleanup should have triggered multiple times
    assert wait_for_cycles(counter, keep_oldest + keep_latest + 5)

    # After cleanup, count should be bounded
    success_count = count_successful_computations(execution.id, :schedule)
    assert success_count <= keep_oldest + keep_latest

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Oldest computations are preserved --

  @tag timeout: 60_000
  test "oldest computations are preserved by retention" do
    keep_latest = 2
    keep_oldest = 3
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test oldest_preserved #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter),
            keep_latest_completed_computations: keep_latest,
            keep_oldest_completed_computations: keep_oldest
          )
        ]
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    # Wait for the first few computations to establish the "oldest" set
    assert wait_for_cycles(counter, 4)
    early_revisions = successful_computation_revisions(execution.id, :schedule)
    oldest_three = Enum.take(early_revisions, keep_oldest)

    # Now wait for many more cycles so cleanup runs multiple times
    assert wait_for_cycles(counter, keep_oldest + keep_latest + 8)

    # The original oldest computations should still be present
    current_revisions = successful_computation_revisions(execution.id, :schedule)

    assert Enum.take(current_revisions, keep_oldest) == oldest_three,
           "Expected oldest #{keep_oldest} revisions #{inspect(oldest_three)} to be preserved, " <>
             "but got #{inspect(current_revisions)}"

    # Total should be bounded
    assert length(current_revisions) <= keep_oldest + keep_latest

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Only :success computations are deleted --

  @tag timeout: 60_000
  test "failed computations are not deleted by retention" do
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test failed_not_deleted #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter),
            keep_latest_completed_computations: 2,
            keep_oldest_completed_computations: 1
          )
        ]
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    # Wait for enough cycles
    assert wait_for_cycles(counter, 6)

    # Manually insert a failed computation for this node
    Journey.Repo.insert!(%Computation{
      execution_id: execution.id,
      node_name: "schedule",
      computation_type: :tick_recurring,
      state: :failed,
      error_details: "test failure"
    })

    # Trigger more cycles so cleanup runs again
    assert wait_for_cycles(counter, 8)

    # The failed computation should still be there
    failed_count =
      from(c in Computation,
        where:
          c.execution_id == ^execution.id and
            c.node_name == "schedule" and
            c.state == :failed
      )
      |> Journey.Repo.aggregate(:count)

    assert failed_count == 1

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Graph-level default applies to downstream compute --

  @tag timeout: 90_000
  test "graph-level keep_latest applies to downstream compute nodes" do
    keep_latest = 3
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test graph_default_downstream #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter)
          ),
          compute(
            :downstream,
            unblocked_when({:schedule, &provided?/1}),
            fn _ -> {:ok, :computed} end
          )
        ],
        keep_latest_completed_computations: keep_latest
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    assert wait_for_cycles(counter, 18, 60_000)

    # Both schedule and downstream should be bounded
    # Default keep_oldest is 10, so max is 10 + keep_latest = 13
    schedule_count = count_successful_computations(execution.id, :schedule)
    downstream_count = count_successful_computations(execution.id, :downstream)

    assert schedule_count <= 10 + keep_latest
    assert downstream_count <= 10 + keep_latest

    stop_background_sweeps_in_test(sweeps)
  end

  # -- Overlapping windows --

  @tag timeout: 60_000
  test "no deletion when total is within retention window" do
    counter = :counters.new(1, [:atomics])

    graph =
      Journey.new_graph(
        "retention_test overlapping_windows #{Random.random_string()}",
        "v1",
        [
          input(:trigger),
          tick_recurring(
            :schedule,
            unblocked_when({:trigger, &true?/1}),
            make_tick_fn(counter),
            keep_latest_completed_computations: 50,
            keep_oldest_completed_computations: 50
          )
        ]
      )

    execution = Journey.start_execution(graph)
    sweeps = start_background_sweeps_in_test(execution.id)

    execution |> Journey.set(:trigger, true)

    assert wait_for_cycles(counter, 5)

    # All should be preserved since 5 < 50 + 50
    success_count = count_successful_computations(execution.id, :schedule)
    assert success_count >= 5

    stop_background_sweeps_in_test(sweeps)
  end
end
