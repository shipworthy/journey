defmodule Journey.Scheduler.ComputeCrossRunFailureTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  alias Journey.Persistence.Schema.Execution.Computation

  describe "cross-run retry-exhaustion" do
    # Compute analog of the loop "cross-run cap-failure" test in
    # test/journey/node/loop/general_test.exs. Pins the same emergent property
    # via the compute failure path: record_error_failed_state in
    # lib/journey/scheduler/completions.ex marks the Computation row :failed on
    # retry-exhaustion without writing to the values table, so a prior Run A
    # success value lingers. Together with the loop test, this documents the
    # invariant from both ends so a future Recompute / Invalidate / failure-path
    # refactor can't silently flip it.
    test "Run A's value lingers when a subsequent run exhausts retries with {:error, _}" do
      graph =
        Journey.new_graph(
          "compute_cross_run_fail_#{random_string()}",
          "v1",
          [
            input(:mode),
            compute(
              :answer,
              [:mode],
              fn values ->
                case values.mode do
                  "succeed" -> {:ok, "run_a_value"}
                  "fail" -> {:error, "run_b_error"}
                end
              end,
              max_retries: 0
            )
          ]
        )

      # Run A: succeed, pinning :answer to "run_a_value".
      execution = graph |> Journey.start_execution() |> Journey.set(:mode, "succeed")
      assert {:ok, "run_a_value", _rev_a} = Journey.get(execution, :answer, wait: :any)

      # Run B: change upstream → compute re-runs, returns {:error, _},
      # max_retries: 0 makes the very first failure terminal.
      # Retry-exhaustion does not write to the value node, so we can't wait
      # on Journey.get; poll the Computation table for the :failed row.
      execution = Journey.set(execution, :mode, "fail")
      _failed_row = wait_for_compute_failed(execution.id, "answer", 10_000)

      # The lingering: Run A's value is still readable.
      execution = Journey.load(execution)
      assert {:ok, "run_a_value", _rev_b} = Journey.get(execution, :answer)
    end
  end

  defp wait_for_compute_failed(execution_id, node_name, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll_compute_failed(execution_id, node_name, deadline)
  end

  defp poll_compute_failed(execution_id, node_name, deadline) do
    row =
      from(c in Computation,
        where:
          c.execution_id == ^execution_id and
            c.node_name == ^node_name and
            c.computation_type == :compute and
            c.state == :failed,
        order_by: [desc: c.inserted_at],
        limit: 1
      )
      |> Journey.Repo.one()

    cond do
      row != nil ->
        row

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("compute node #{node_name} did not reach :failed within deadline")

      true ->
        Process.sleep(50)
        poll_compute_failed(execution_id, node_name, deadline)
    end
  end
end
