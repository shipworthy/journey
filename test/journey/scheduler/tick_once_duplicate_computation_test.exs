defmodule Journey.Scheduler.TickOnceDuplicateComputationTest do
  @moduledoc """
  Reproduction test for Bug #1: Background sweeper creating duplicate tick_once computations.

  The bug report claims that after a single set_values call, the tick_once computation
  function can be called twice with identical inputs, producing two separate revisions.
  This breaks `wait: {:newer_than, revision}` semantics.

  The key insight from the reporter is that the bug occurs with a complex cascading graph
  where multiple parallel branches are triggered by the same set_values call. Each branch
  completion triggers recursive advance() calls, creating many concurrent advance() calls
  that might race with each other.
  """
  use ExUnit.Case, async: true

  import Ecto.Query
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies
  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  require Logger

  alias Journey.Persistence.Schema.Execution.Computation

  # ── helpers ──────────────────────────────────────────────────────────────

  defp count_computations(execution_id, node_name) do
    from(c in Computation,
      where: c.execution_id == ^execution_id and c.node_name == ^Atom.to_string(node_name)
    )
    |> Journey.Repo.all()
    |> length()
  end

  defp count_successful_computations(execution_id, node_name) do
    from(c in Computation,
      where:
        c.execution_id == ^execution_id and
          c.node_name == ^Atom.to_string(node_name) and
          c.state == ^:success
    )
    |> Journey.Repo.all()
    |> length()
  end

  # ── graph builders ─────────────────────────────────────────────────────

  # Simple graph: just tick_once with two dependencies
  defp build_simple_graph(counter_agent) do
    graph_name = "tick_once_dup_simple_#{random_string()}"

    Journey.new_graph(
      graph_name,
      "1.0.0",
      [
        input(:due_date),
        input(:due_type),
        tick_once(
          :due_date_reminder,
          [:due_date, :due_type],
          fn %{due_date: due_date, due_type: _due_type} ->
            Agent.update(counter_agent, &(&1 + 1))
            count = Agent.get(counter_agent, & &1)

            Logger.warning(
              "DIAG[simple] tick_once f_compute invoked (call ##{count}), " <>
                "due_date=#{due_date}, pid=#{inspect(self())}"
            )

            {:ok, due_date - 86_400}
          end
        )
      ]
    )
  end

  # Cascade graph: mimics the Ooshki graph topology where set_values triggers
  # multiple parallel branches, each with cascading computations.
  #
  # set_values(%{due_type: "on", due_date: ..., contents: "..."})
  #   ├── Branch 1: item_changes → item_history → schedule_notification (tick_once) → send_notification
  #   └── Branch 2: due_date_reminder (tick_once) ← THE VICTIM
  #
  # Each branch completion triggers recursive advance() calls. The hypothesis is that
  # these concurrent cascading advance() calls can cause Recompute to see stale
  # computed_with data and create a duplicate tick_once computation.
  defp build_cascade_graph(counter_agent) do
    graph_name = "tick_once_dup_cascade_#{random_string()}"

    Journey.new_graph(
      graph_name,
      "1.0.0",
      [
        input(:due_date),
        input(:due_type),
        input(:contents),

        # Branch 1: deep cascade triggered by same inputs
        historian(
          :item_changes,
          unblocked_when({
            :or,
            [
              {:contents, &provided?/1},
              {:due_type, &provided?/1},
              {:due_date, &provided?/1}
            ]
          })
        ),
        compute(
          :item_history,
          unblocked_when({
            :or,
            [{:item_changes, &provided?/1}]
          }),
          fn %{item_changes: changes} ->
            Logger.warning("DIAG[cascade] item_history computed, changes=#{inspect(length(changes || []))}")
            {:ok, changes}
          end
        ),
        tick_once(
          :schedule_notification,
          [:item_history],
          fn %{item_history: _history} ->
            Logger.warning("DIAG[cascade] schedule_notification tick_once computed")
            # Schedule far in the future
            {:ok, System.system_time(:second) + 100_000}
          end
        ),
        compute(
          :send_notification,
          [:schedule_notification],
          fn %{schedule_notification: _time} ->
            Logger.warning("DIAG[cascade] send_notification computed")
            {:ok, "notification_sent"}
          end
        ),

        # Branch 2: the victim — tick_once that should only fire once per upstream change
        tick_once(
          :due_date_reminder,
          [:due_date, :due_type],
          fn %{due_date: due_date, due_type: _due_type} ->
            Agent.update(counter_agent, &(&1 + 1))
            count = Agent.get(counter_agent, & &1)

            Logger.warning(
              "DIAG[cascade] due_date_reminder tick_once invoked (call ##{count}), " <>
                "due_date=#{due_date}, pid=#{inspect(self())}"
            )

            {:ok, due_date - 86_400}
          end
        )
      ]
    )
  end

  # ── tests: simple graph ────────────────────────────────────────────────

  describe "simple graph: single set_values should produce exactly one tick_once computation" do
    test "with background sweeps active" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      graph = build_simple_graph(counter)
      execution = Journey.start_execution(graph)
      sweeps_pid = start_background_sweeps_in_test(execution.id)

      far_future = System.system_time(:second) + 500_000
      execution = Journey.set(execution, :due_type, "on")
      execution = Journey.set(execution, :due_date, far_future)

      {:ok, value, initial_revision} = Journey.get(execution, :due_date_reminder, wait: :any)
      assert value == far_future - 86_400

      Process.sleep(3_000)
      assert Agent.get(counter, & &1) == 1

      # Second set
      new_future = System.system_time(:second) + 800_000
      execution = Journey.set(execution, :due_date, new_future)

      {:ok, new_value, new_revision} =
        Journey.get(execution, :due_date_reminder,
          wait: {:newer_than, initial_revision},
          timeout: 10_000
        )

      assert new_value == new_future - 86_400
      assert new_revision > initial_revision

      Process.sleep(2_000)
      assert Agent.get(counter, & &1) == 2

      stop_background_sweeps_in_test(sweeps_pid)
      Agent.stop(counter)
    end

    test "10 concurrent advance() calls" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      graph = build_simple_graph(counter)
      execution = Journey.start_execution(graph)

      far_future = System.system_time(:second) + 500_000
      execution = Journey.set(execution, :due_type, "on")
      execution = Journey.set(execution, :due_date, far_future)

      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            Journey.load(execution.id) |> Journey.Scheduler.advance()
          end)
        end

      Enum.each(tasks, &Task.await(&1, 15_000))

      {:ok, value, _rev} = Journey.get(execution, :due_date_reminder, wait: :any, timeout: 10_000)
      assert value == far_future - 86_400

      Process.sleep(2_000)

      assert Agent.get(counter, & &1) == 1,
             "Expected 1 invocation with 10 concurrent advance(), got #{Agent.get(counter, & &1)}"

      Agent.stop(counter)
    end
  end

  # ── tests: cascade graph (the real scenario) ───────────────────────────

  describe "cascade graph: parallel branches with recursive advance() calls" do
    test "single set_values triggers due_date_reminder exactly once" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      graph = build_cascade_graph(counter)
      execution = Journey.start_execution(graph)
      sweeps_pid = start_background_sweeps_in_test(execution.id)

      # Set all three inputs — triggers both branches simultaneously
      far_future = System.system_time(:second) + 500_000
      execution = Journey.set(execution, :due_type, "on")
      execution = Journey.set(execution, :contents, "Test item")
      execution = Journey.set(execution, :due_date, far_future)

      # Wait for due_date_reminder to compute
      {:ok, value, initial_revision} =
        Journey.get(execution, :due_date_reminder, wait: :any, timeout: 15_000)

      assert value == far_future - 86_400

      # Wait for the entire cascade to complete (item_changes → item_history →
      # schedule_notification → send_notification), each of which triggers
      # recursive advance() calls that run Recompute on ALL nodes
      Process.sleep(5_000)

      invocations = Agent.get(counter, & &1)
      total = count_computations(execution.id, :due_date_reminder)
      successful = count_successful_computations(execution.id, :due_date_reminder)

      Logger.warning(
        "DIAG[cascade] after initial set + cascade: " <>
          "invocations=#{invocations}, total=#{total}, successful=#{successful}"
      )

      assert invocations == 1,
             "Expected exactly 1 due_date_reminder invocation after initial set, " <>
               "got #{invocations}. total_computations=#{total}, successful=#{successful}"

      # ── second set: change due_date (re-triggers both branches) ──

      new_future = System.system_time(:second) + 800_000

      Logger.warning("DIAG[cascade] setting due_date to #{new_future}, initial_revision=#{initial_revision}")

      execution = Journey.set(execution, :due_date, new_future)

      {:ok, new_value, new_revision} =
        Journey.get(execution, :due_date_reminder,
          wait: {:newer_than, initial_revision},
          timeout: 15_000
        )

      assert new_value == new_future - 86_400,
             "Expected reminder for new due_date, got #{new_value} " <>
               "(possible stale duplicate revision)"

      assert new_revision > initial_revision

      # Wait for cascade to complete again
      Process.sleep(5_000)

      final_invocations = Agent.get(counter, & &1)
      final_total = count_computations(execution.id, :due_date_reminder)
      final_successful = count_successful_computations(execution.id, :due_date_reminder)

      Logger.warning(
        "DIAG[cascade] final: invocations=#{final_invocations}, " <>
          "total=#{final_total}, successful=#{final_successful}"
      )

      assert final_invocations == 2,
             "Expected exactly 2 due_date_reminder invocations total, " <>
               "got #{final_invocations}. total=#{final_total}, successful=#{final_successful}"

      stop_background_sweeps_in_test(sweeps_pid)
      Agent.stop(counter)
    end

    test "cascade without sweeps — isolates cascade-triggered advance() races" do
      # No background sweeps. If this test fails, the bug is in the cascade advance()
      # calls alone, not the sweeper interaction.
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      graph = build_cascade_graph(counter)
      execution = Journey.start_execution(graph)

      far_future = System.system_time(:second) + 500_000
      execution = Journey.set(execution, :due_type, "on")
      execution = Journey.set(execution, :contents, "Test item")
      execution = Journey.set(execution, :due_date, far_future)

      {:ok, value, initial_revision} =
        Journey.get(execution, :due_date_reminder, wait: :any, timeout: 15_000)

      assert value == far_future - 86_400

      # Wait for cascade to settle
      Process.sleep(5_000)

      assert Agent.get(counter, & &1) == 1,
             "Expected 1 invocation (no sweeps), got #{Agent.get(counter, & &1)}"

      # Second set
      new_future = System.system_time(:second) + 800_000
      execution = Journey.set(execution, :due_date, new_future)

      {:ok, new_value, _new_revision} =
        Journey.get(execution, :due_date_reminder,
          wait: {:newer_than, initial_revision},
          timeout: 15_000
        )

      assert new_value == new_future - 86_400

      Process.sleep(5_000)

      assert Agent.get(counter, & &1) == 2,
             "Expected 2 invocations (no sweeps), got #{Agent.get(counter, & &1)}"

      Agent.stop(counter)
    end

    test "10 concurrent advance() with cascade graph" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      graph = build_cascade_graph(counter)
      execution = Journey.start_execution(graph)

      far_future = System.system_time(:second) + 500_000
      execution = Journey.set(execution, :due_type, "on")
      execution = Journey.set(execution, :contents, "Test item")
      execution = Journey.set(execution, :due_date, far_future)

      # Fire 10 concurrent advance() calls on top of the cascade
      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            Journey.load(execution.id) |> Journey.Scheduler.advance()
          end)
        end

      Enum.each(tasks, &Task.await(&1, 15_000))

      {:ok, value, _rev} =
        Journey.get(execution, :due_date_reminder, wait: :any, timeout: 15_000)

      assert value == far_future - 86_400

      # Wait for full cascade + any duplicates
      Process.sleep(5_000)

      invocations = Agent.get(counter, & &1)
      total = count_computations(execution.id, :due_date_reminder)

      Logger.warning("DIAG[cascade+concurrent] invocations=#{invocations}, total=#{total}")

      assert invocations == 1,
             "Expected 1 due_date_reminder invocation with cascade + 10 concurrent advance(), " <>
               "got #{invocations}. total_computations=#{total}"

      Agent.stop(counter)
    end
  end

  # ── tests: wait: {:newer_than, revision} correctness ───────────────────

  describe "wait: {:newer_than, revision} returns correct value after recomputation" do
    test "does not return stale duplicate revision (cascade graph)" do
      # The exact failure from the bug report:
      # 1. First set → tick_once computes → revision A
      # 2. Capture initial_revision = A
      # 3. (Sweeper/cascade duplicate) → same value → revision B (B > A)
      # 4. Second set → tick_once computes → revision C (new value)
      # 5. get(wait: {:newer_than, A}) finds B → returns OLD value instead of C

      {:ok, counter} = Agent.start_link(fn -> 0 end)
      graph = build_cascade_graph(counter)
      execution = Journey.start_execution(graph)
      sweeps_pid = start_background_sweeps_in_test(execution.id)

      # Step 1: First set (triggers both branches)
      first_due_date = System.system_time(:second) + 500_000
      execution = Journey.set(execution, :due_type, "on")
      execution = Journey.set(execution, :contents, "Test item")
      execution = Journey.set(execution, :due_date, first_due_date)

      {:ok, first_value, first_revision} =
        Journey.get(execution, :due_date_reminder, wait: :any, timeout: 15_000)

      assert first_value == first_due_date - 86_400

      # Step 2: Wait for cascade + sweeper window
      Process.sleep(3_000)

      # Step 3: Second set with different due_date
      second_due_date = System.system_time(:second) + 900_000
      execution = Journey.set(execution, :due_date, second_due_date)

      # Step 4: Wait for newer revision
      {:ok, result_value, result_revision} =
        Journey.get(execution, :due_date_reminder,
          wait: {:newer_than, first_revision},
          timeout: 15_000
        )

      expected = second_due_date - 86_400

      assert result_value == expected,
             "RACE CONDITION DETECTED: got #{result_value} " <>
               "(expected #{expected} from second set, first set produced #{first_value}). " <>
               "Revision: #{first_revision} → #{result_revision}. " <>
               "Invocations: #{Agent.get(counter, & &1)}"

      assert result_revision > first_revision

      stop_background_sweeps_in_test(sweeps_pid)
      Agent.stop(counter)
    end
  end
end
