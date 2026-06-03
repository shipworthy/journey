defmodule Journey.Scheduler.RemovedNodeSweepTest do
  # Removing a node from a graph definition (while keeping the same name + version) should be a
  # safe operation for executions that still carry rows for the removed node. The scheduler must
  # treat those orphaned rows as inert (noop + info log) rather than crashing.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Ecto.Query
  import Journey.Node
  import Journey.Helpers.Random

  alias Journey.Persistence.Schema.Execution.Value

  test "removing a self-computing node does not crash a subsequent sweep" do
    graph_name = "removed_node_#{random_string()}"

    # v1: input :a feeds self-computing :b.
    graph_v1 =
      Journey.new_graph(graph_name, "v1.0.0", [
        input(:a),
        compute(:b, [:a], fn %{a: a} -> {:ok, "computed from #{a}"} end)
      ])

    execution = Journey.start_execution(graph_v1)
    execution = Journey.set(execution, :a, "hello")

    {:ok, "computed from hello", _} = Journey.get(execution, :b, wait: :any, timeout: 10_000)

    # v2: same name + version, :b removed. Re-registering overwrites the catalog entry.
    _graph_v2 = Journey.new_graph(graph_name, "v1.0.0", [input(:a)])

    # The scoped sweep must not crash on the now-orphaned :b computation row, and should note it.
    execution = Journey.load(execution.id)

    log =
      capture_log(fn ->
        Journey.Scheduler.advance(execution)
      end)

    assert log =~ "no longer in the graph"
    assert log =~ ":b"

    # The orphaned value row for :b is left untouched (inert), preserving history.
    b_value =
      Journey.Repo.one(from(v in Value, where: v.execution_id == ^execution.id and v.node_name == "b"))

    assert b_value != nil
    assert b_value.node_value == "computed from hello"
  end

  test "completing an in-flight computation for a removed node does not crash" do
    graph_name = "removed_node_inflight_#{random_string()}"
    test_pid = self()

    # :b parks until the test releases it, so its computation is still in flight when we remove it.
    graph_v1 =
      Journey.new_graph(graph_name, "v1.0.0", [
        input(:a),
        compute(:b, [:a], fn %{a: a} ->
          send(test_pid, {:b_computing, self()})

          receive do
            :proceed -> {:ok, "computed from #{a}"}
          end
        end)
      ])

    execution = Journey.start_execution(graph_v1)
    # set/3 advances the execution, which grabs :b and spawns its worker; the worker then parks.
    Journey.set(execution, :a, "hello")

    assert_receive {:b_computing, worker_pid}, 5_000

    # v2: same name + version, :b removed while its computation is still in flight.
    _graph_v2 = Journey.new_graph(graph_name, "v1.0.0", [input(:a)])

    # Unblock the worker and let it finish. It records its result against a graph that no longer has
    # :b, which must not crash -- we expect a graceful info log and no error/crash report.
    log =
      capture_log(fn ->
        send(worker_pid, :proceed)
        wait_until_finished(worker_pid)
      end)

    assert log =~ "no longer in graph"
    refute log =~ "[error]"
  end

  # Wait for `pid` to finish, so the log it emits while completing is captured before we inspect it.
  defp wait_until_finished(pid, remaining_ms \\ 5_000)
  defp wait_until_finished(_pid, remaining_ms) when remaining_ms <= 0, do: :timeout

  defp wait_until_finished(pid, remaining_ms) do
    if Process.alive?(pid) do
      sleep_ms = 100
      Process.sleep(sleep_ms)
      wait_until_finished(pid, remaining_ms - sleep_ms)
    else
      :ok
    end
  end
end
