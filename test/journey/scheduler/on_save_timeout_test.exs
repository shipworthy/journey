defmodule Journey.Scheduler.OnSaveTimeoutTest do
  # async: false — the abandoned sweeper uses a global SweepRun throttle table; running in parallel
  # with other sweep-driving tests would race on row deletes/inserts.
  use ExUnit.Case, async: false

  import Ecto.Query
  import Journey.Node

  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.Abandoned

  setup do
    Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :abandoned))
    :ok
  end

  @tag :capture_log
  test "compute fires f_on_save once with {:error, \"timeout\"} when retries exhaust via abandonment" do
    test_pid = self()

    # Worker sleep window is sized to land *after* the manual sweep + assert_receive but *inside*
    # the refute window below — so the refute actually exercises the no-double-fire path when the
    # late worker wakes, calls record_success, and hits the state != :computing early-exit in
    # record_success_in_transaction. Sweep happens at t≈2s; refute window runs t≈2s→4s; worker
    # waking at t≈3s lands inside the refute window so the no-double-fire property is exercised.
    worker_sleep_ms = 3_000

    graph =
      Journey.new_graph(
        "compute timeout #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:trigger),
          compute(
            :sleeper,
            [:trigger],
            fn _ ->
              Process.sleep(worker_sleep_ms)
              {:ok, "should never reach here"}
            end,
            abandon_after_seconds: 1,
            # max_retries: 0 means the very first abandonment is terminal (count=1, 1 < 0 false).
            max_retries: 0,
            f_on_save: fn _execution_id, node_name, result ->
              send(test_pid, {:cb, node_name, result})
              :ok
            end
          )
        ]
      )

    execution = Journey.start_execution(graph)
    Journey.set(execution, :trigger, "go")

    # Wait past abandon_after_seconds so the deadline is in the past at sweep time. The periodic
    # sweeper runs in :manual mode under Mix.env() == :test (see Periodic.@mode), so nothing
    # inserts SweepRun rows during this sleep — the setup-block delete handles stale rows.
    Process.sleep(2_000)

    current_time = System.system_time(:second)
    assert {kicked, _sweep_id} = Abandoned.sweep(execution.id, current_time)
    # Tighten to >= 1: a throttled-sweep return of {0, nil} would otherwise pass through here and
    # fail at assert_receive below with a misleading "no callback received" error.
    assert kicked >= 1

    # Callback fires post-commit from the sweeper with {:error, "timeout"}.
    assert_receive {:cb, :sleeper, {:error, "timeout"}}, 5_000

    # Refute window must span past the worker's wake at ~t=worker_sleep_ms after start. This
    # exercises the no-double-fire architectural property: the late worker calls record_success,
    # finds state == :abandoned, takes the early-exit, returns :no_value_written, and the gate
    # stays silent.
    refute_receive {:cb, :sleeper, _}, 2_000
  end
end
