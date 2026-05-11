defmodule Journey.Scheduler.OnSaveTickTimeoutTest do
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
  test "tick_once fires f_on_save once with {:error, \"timeout\"} when retries exhaust via abandonment" do
    test_pid = self()

    # Worker sleep window is sized to land *after* the manual sweep + assert_receive but *inside*
    # the refute window below — so the refute actually exercises the no-double-fire path when the
    # late worker wakes, calls record_success, and hits the state != :computing early-exit in
    # record_success_in_transaction.
    worker_sleep_ms = 5_000

    graph =
      Journey.new_graph(
        "tick_once timeout #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:trigger),
          tick_once(
            :sleeper,
            [:trigger],
            fn _ ->
              Process.sleep(worker_sleep_ms)
              {:ok, System.system_time(:second) + 60}
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

    # Wait past abandon_after_seconds so the deadline is in the past at sweep time.
    Process.sleep(2_000)

    current_time = System.system_time(:second)
    assert {kicked, _sweep_id} = Abandoned.sweep(execution.id, current_time)
    assert kicked >= 1

    assert_receive {:cb, :sleeper, {:error, "timeout"}}, 5_000

    # Refute window spans past the worker's wake at ~t=worker_sleep_ms after start. Exercises the
    # no-double-fire architectural property: late worker calls record_success, finds state ==
    # :abandoned, takes the early-exit, returns :no_value_written, and the gate stays silent.
    refute_receive {:cb, :sleeper, _}, 4_000
  end
end
