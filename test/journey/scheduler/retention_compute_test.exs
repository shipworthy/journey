defmodule Journey.Scheduler.RetentionComputeTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import Journey.Node

  alias Journey.Helpers.Random
  alias Journey.Persistence.Schema.Execution.Computation

  # Retention runs outside the completion transaction (see retention.ex:1-4). During
  # active ticking, the visible count can transiently exceed the configured cap by the
  # number of in-flight completions. This affordance absorbs that race in assertions
  # taken while ticking is ongoing. A real regression (broken retention) would show up
  # as unbounded growth, well past any reasonable affordance.
  @eventual_retention_affordance 3

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

  defp trigger_compute_cycles(execution, n) do
    Enum.reduce(1..n, 0, fn i, prev_rev ->
      Journey.set(execution, :input_val, i)
      Process.sleep(50)
      {:ok, _, rev} = Journey.get(execution, :derived, wait: {:newer_than, prev_rev})
      rev
    end)
  end

  test "retention bounds compute node after many sets" do
    keep_latest = 3
    keep_oldest = 2

    graph =
      Journey.new_graph(
        "retention_compute_test bounds #{Random.random_string()}",
        "v1",
        [
          input(:input_val),
          compute(
            :derived,
            [:input_val],
            fn %{input_val: v} -> {:ok, v * 10} end,
            keep_latest_completed_computations: keep_latest,
            keep_oldest_completed_computations: keep_oldest
          )
        ]
      )

    execution = Journey.start_execution(graph)

    trigger_compute_cycles(execution, 20)

    success_count = count_successful_computations(execution.id, :derived)
    assert success_count <= keep_oldest + keep_latest + @eventual_retention_affordance
  end

  test "oldest computations are preserved after many sets" do
    keep_latest = 2
    keep_oldest = 3

    graph =
      Journey.new_graph(
        "retention_compute_test oldest_preserved #{Random.random_string()}",
        "v1",
        [
          input(:input_val),
          compute(
            :derived,
            [:input_val],
            fn %{input_val: v} -> {:ok, v * 10} end,
            keep_latest_completed_computations: keep_latest,
            keep_oldest_completed_computations: keep_oldest
          )
        ]
      )

    execution = Journey.start_execution(graph)

    # Run a few cycles to establish the oldest set
    trigger_compute_cycles(execution, 5)
    early_revisions = successful_computation_revisions(execution.id, :derived)
    oldest_expected = Enum.take(early_revisions, keep_oldest)
    assert length(oldest_expected) == keep_oldest

    # Run many more cycles
    trigger_compute_cycles(execution, 20)

    current_revisions = successful_computation_revisions(execution.id, :derived)

    assert Enum.take(current_revisions, keep_oldest) == oldest_expected,
           "Expected oldest #{keep_oldest} revisions #{inspect(oldest_expected)} to be preserved, " <>
             "got #{inspect(current_revisions)}"

    assert length(current_revisions) <= keep_oldest + keep_latest + @eventual_retention_affordance
  end

  test "default keep_oldest is 10 when only keep_latest is set" do
    keep_latest = 2

    graph =
      Journey.new_graph(
        "retention_compute_test default_keep_oldest #{Random.random_string()}",
        "v1",
        [
          input(:input_val),
          compute(
            :derived,
            [:input_val],
            fn %{input_val: v} -> {:ok, v} end,
            keep_latest_completed_computations: keep_latest
          )
        ]
      )

    execution = Journey.start_execution(graph)

    trigger_compute_cycles(execution, 25)

    # Default keep_oldest is 10, so max is 10 + 2 = 12
    success_count = count_successful_computations(execution.id, :derived)
    assert success_count <= 12 + @eventual_retention_affordance

    # Verify the oldest and latest are distinct groups
    revisions = successful_computation_revisions(execution.id, :derived)
    assert length(revisions) >= 10
    oldest_10 = Enum.take(revisions, 10)
    latest_2 = Enum.take(revisions, -keep_latest)
    assert Enum.max(oldest_10) < Enum.min(latest_2)
  end

  test "no cleanup when retention is not configured" do
    graph =
      Journey.new_graph(
        "retention_compute_test no_cleanup #{Random.random_string()}",
        "v1",
        [
          input(:input_val),
          compute(
            :derived,
            [:input_val],
            fn %{input_val: v} -> {:ok, v} end
          )
        ]
      )

    execution = Journey.start_execution(graph)

    trigger_compute_cycles(execution, 15)

    success_count = count_successful_computations(execution.id, :derived)
    assert success_count >= 15
  end

  test "graph-level retention applies to compute node" do
    keep_latest = 3

    graph =
      Journey.new_graph(
        "retention_compute_test graph_level #{Random.random_string()}",
        "v1",
        [
          input(:input_val),
          compute(
            :derived,
            [:input_val],
            fn %{input_val: v} -> {:ok, v} end
          )
        ],
        keep_latest_completed_computations: keep_latest
      )

    execution = Journey.start_execution(graph)

    trigger_compute_cycles(execution, 20)

    # Default keep_oldest: 10, keep_latest: 3, max 13
    success_count = count_successful_computations(execution.id, :derived)
    assert success_count <= 10 + keep_latest + @eventual_retention_affordance
  end

  test "node :all overrides graph-level retention on compute" do
    graph =
      Journey.new_graph(
        "retention_compute_test node_all_override #{Random.random_string()}",
        "v1",
        [
          input(:input_val),
          compute(
            :derived,
            [:input_val],
            fn %{input_val: v} -> {:ok, v} end,
            keep_latest_completed_computations: :all
          )
        ],
        keep_latest_completed_computations: 2
      )

    execution = Journey.start_execution(graph)

    trigger_compute_cycles(execution, 15)

    success_count = count_successful_computations(execution.id, :derived)
    assert success_count >= 15
  end
end
