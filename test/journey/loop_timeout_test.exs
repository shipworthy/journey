defmodule Journey.LoopTimeoutTest do
  # async: false — the abandoned sweeper uses a global SweepRun throttle table; running
  # in parallel with other sweep-driving tests would race on row deletes/inserts. Same
  # reason as on_save_timeout_test.exs. Kept in its own file so loop_test.exs (30+ tests)
  # can stay async: true.
  use ExUnit.Case, async: false

  import Ecto.Query
  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node
  import WaitForIt

  alias Journey.Persistence.Schema.Execution.Computation
  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.Abandoned

  setup do
    Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :abandoned))
    :ok
  end

  # Verifies that the per-iteration retry budget is per-iteration when iterations
  # are abandoned via :abandon_after_seconds (rather than failing with `{:error, _}`
  # from the step function). Companion to "iter 2's retry budget is independent of
  # iter 1's success" in loop_test.exs — same property, different retry trigger.
  #
  # max_retries: 2 is load-bearing for discrimination (see the loop_test.exs
  # companion for the full reasoning). A single Abandoned.sweep call is sufficient
  # because the sweep's kick_all_executions_for_these_computations advances the
  # retry chain via scheduler kicks, not subsequent sweeps — so the
  # min_seconds_between_runs throttle is irrelevant. If this test ever flakes, look
  # at the kick chain first, not retry counting.
  @tag :capture_log
  test "iter 2's retry budget after abandonment is independent of iter 1's success" do
    counter = :counters.new(1, [])

    graph =
      Journey.new_graph(
        "loop_timeout_per_iter_budget_#{random_string()}",
        "v1",
        [
          loop(
            :answer,
            [],
            fn values ->
              # Self-reference round-trips through loop_state (:map → JSON), so use
              # strings, not atoms, for stable matching.
              case values[:answer] do
                nil ->
                  # Iter 1: complete immediately, no abandonment.
                  {:cont_with_fallback, "iter_1_done"}

                "iter_1_done" ->
                  # Iter 2: first call sleeps for 5s — well past abandon_after_seconds: 1
                  # AND well past the test's Process.sleep(2_000) below, so the row is
                  # unambiguously :computing when the sweep runs at t=~2s. A shorter sleep
                  # (e.g. 2s) creates a race where the worker may wake at the same time as
                  # the sweep and update the row to :success before the sweep's FOR UPDATE
                  # lock acquires it — sweep then finds nothing to abandon.
                  # The original worker wakes at t=~5s, sees state :abandoned (set by the
                  # sweep), early-exits in record_success_in_transaction — no double-write.
                  # Retry call returns :ok immediately.
                  n = :counters.get(counter, 1)
                  :counters.add(counter, 1, 1)

                  if n == 0 do
                    Process.sleep(5_000)
                    {:ok, "should-not-reach-here"}
                  else
                    {:ok, "terminal"}
                  end
              end
            end,
            max_iterations: 5,
            max_retries: 2,
            abandon_after_seconds: 1
          )
        ]
      )

    execution = Journey.start_execution(graph)

    # Poll for iter 2 to reach :computing — the deterministic equivalent of
    # waiting wall-clock time for the worker to start. Typically lands in <200ms.
    iter_2_row = wait_for_loop_row_in_state(execution.id, 2, :computing)

    assert match?(%Computation{}, iter_2_row),
           "iter 2 never reached :computing state within 5s; got #{inspect(iter_2_row)}"

    # Force iter 2's deadline into the past so the abandoned sweep finds it without
    # us waiting for wall-clock to advance. We can't pass a synthetic current_time
    # to the sweep instead: process_until_done loops with the same current_time, and
    # a synthetic future time would also match retry rows inserted mid-sweep
    # (their deadlines are real_now + 1) — abandoning them too and exhausting the
    # budget on the spot.
    #
    # Real wall-clock current_time below avoids that interaction. Retries land with
    # deadline real_now + 1, sweep filters by deadline < real_now → retries don't
    # match. Direct DB manipulation in tests follows the established pattern in
    # recompute_stops_after_max_retries_test.exs.
    {1, _} =
      from(c in Computation, where: c.id == ^iter_2_row.id)
      |> Journey.Repo.update_all(set: [deadline: 0])

    current_time = System.system_time(:second)
    assert {kicked, _sweep_id} = Abandoned.sweep(execution.id, current_time)
    assert kicked >= 1, "expected sweep to kick at least one execution; got #{kicked}"

    # Under broken code: count = iter 1 :success + iter 2 :abandoned = 2 rows,
    # 2 < 2 false → exhausted on first abandonment, no retry, terminal value never set,
    # this assertion times out.
    # Under fixed code: count scoped to iter 2 = 1 row, 1 < 2 → retry scheduled,
    # retry returns :ok via the scheduler kick chain.
    assert {:ok, "terminal", _} =
             Journey.get(execution, :answer, wait: :any, timeout: 10_000)

    # Diagnostic row count: iter 2 has 2 rows (1 :abandoned, 1 :success). If this
    # regresses, the row counts disambiguate "abandonment never happened" from
    # "abandonment happened but retry was never scheduled."
    rows_by_iter =
      from(c in Computation,
        where:
          c.execution_id == ^execution.id and
            c.node_name == "answer" and
            c.computation_type == :loop and
            c.state in [:success, :abandoned],
        select: {c.loop_iteration, c.state}
      )
      |> Journey.Repo.all()
      |> Enum.frequencies()

    assert rows_by_iter == %{
             {1, :success} => 1,
             {2, :abandoned} => 1,
             {2, :success} => 1
           }
  end

  # Polls for a :loop computation row matching (execution_id, iteration, state) to exist.
  # `node_name == "answer"` is hardcoded because both companion tests use the
  # `:answer` node from `single_loop_graph`; generalize if a future test needs it.
  defp wait_for_loop_row_in_state(execution_id, iteration, state) do
    query =
      from(c in Computation,
        where:
          c.execution_id == ^execution_id and
            c.node_name == "answer" and
            c.loop_iteration == ^iteration and
            c.state == ^state,
        limit: 1
      )

    wait(Journey.Repo.one(query), timeout: 5_000, frequency: 50)
  end
end
