defmodule Journey.LoopTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  alias Journey.Persistence.Schema.Execution.Computation

  describe "loop/4 construction-time validation" do
    test "rejects missing :max_iterations" do
      assert_raise ArgumentError, ~r/requires the :max_iterations option/, fn ->
        loop(:l, [], fn _ -> {:ok, 1} end)
      end
    end

    test "rejects zero" do
      assert_raise ArgumentError, ~r/positive integer/, fn ->
        loop(:l, [], fn _ -> {:ok, 1} end, max_iterations: 0)
      end
    end

    test "rejects negative" do
      assert_raise ArgumentError, ~r/positive integer/, fn ->
        loop(:l, [], fn _ -> {:ok, 1} end, max_iterations: -1)
      end
    end

    test "rejects :infinity" do
      assert_raise ArgumentError, ~r/positive integer/, fn ->
        loop(:l, [], fn _ -> {:ok, 1} end, max_iterations: :infinity)
      end
    end

    test "rejects unknown options" do
      assert_raise ArgumentError, ~r/Unknown options/, fn ->
        loop(:l, [], fn _ -> {:ok, 1} end, max_iterations: 5, bogus: true)
      end
    end

    test "accepts a valid construction" do
      step = loop(:l, [], fn _ -> {:ok, 1} end, max_iterations: 5)
      assert step.type == :loop
      assert step.max_iterations == 5
      assert step.max_retries == 3
      assert step.abandon_after_seconds == 60
    end
  end

  describe "happy path" do
    test "{:ok, value} terminates with that value as the loop's terminal value" do
      graph =
        single_loop_graph(
          "ok_returns_terminal_#{random_string()}",
          fn _values ->
            {:ok, "answer"}
          end,
          max_iterations: 5
        )

      execution = graph |> Journey.start_execution()
      assert {:ok, "answer", _rev} = Journey.get(execution, :answer, wait: :any)
    end

    test "self-reference threads through iterations and {:ok, _} terminates" do
      # The step function builds an accumulating list. After 3 :cont_with_fallback
      # iterations the list has 3 elements; the 4th call returns :ok.
      graph =
        single_loop_graph(
          "self_ref_#{random_string()}",
          fn values ->
            state = values[:answer] || []

            if length(state) >= 3 do
              {:ok, state}
            else
              {:cont_with_fallback, [length(state) | state]}
            end
          end,
          max_iterations: 10
        )

      execution = graph |> Journey.start_execution()
      assert {:ok, [2, 1, 0], _rev} = Journey.get(execution, :answer, wait: :any)
    end

    test "arity-1 and arity-2 step functions both work" do
      graph_arity_1 =
        single_loop_graph(
          "arity1_#{random_string()}",
          fn _values ->
            {:ok, :a1}
          end,
          max_iterations: 1
        )

      graph_arity_2 =
        single_loop_graph(
          "arity2_#{random_string()}",
          fn _values, _value_nodes_map ->
            {:ok, :a2}
          end,
          max_iterations: 1
        )

      e1 = graph_arity_1 |> Journey.start_execution()
      e2 = graph_arity_2 |> Journey.start_execution()

      assert {:ok, "a1", _} = Journey.get(e1, :answer, wait: :any)
      assert {:ok, "a2", _} = Journey.get(e2, :answer, wait: :any)
    end

    test "self-reference is not externally visible during iteration" do
      # The step function never returns :ok within the cap, so the values table is
      # never written. Journey.values/1 should not surface the loop's accumulating state.
      graph =
        single_loop_graph(
          "not_visible_#{random_string()}",
          fn values ->
            state = values[:answer] || 0
            {:cont_no_fallback, state + 1}
          end,
          max_iterations: 2
        )

      execution = graph |> Journey.start_execution()

      # Wait for the loop to exhaust iterations
      :timer.sleep(2_000)
      execution = Journey.load(execution)
      values = Journey.values(execution)

      refute Map.has_key?(values, :answer)
    end
  end

  describe "cap behavior" do
    test ":cont_with_fallback at cap promotes the carried value" do
      # max_iterations: 2; the step function always returns :cont_with_fallback.
      # After the 2nd iteration (current_iter == max_iter), the value is promoted.
      graph =
        single_loop_graph(
          "cap_promote_#{random_string()}",
          fn values ->
            n = values[:answer] || 0
            {:cont_with_fallback, n + 1}
          end,
          max_iterations: 2
        )

      execution = graph |> Journey.start_execution()
      assert {:ok, 2, _} = Journey.get(execution, :answer, wait: :any)
    end

    test ":cont_no_fallback at cap leaves the loop's value unset" do
      graph =
        single_loop_graph(
          "cap_fail_#{random_string()}",
          fn values ->
            n = values[:answer] || 0
            {:cont_no_fallback, n + 1}
          end,
          max_iterations: 2
        )

      execution = graph |> Journey.start_execution()
      # Wait long enough for both iterations to complete.
      :timer.sleep(2_000)
      execution = Journey.load(execution)

      assert {:error, :not_set} = Journey.get(execution, :answer)

      # And introspection: the last successful iteration row carries the loop_state for debugging.
      last_iter =
        from(c in Computation,
          where:
            c.execution_id == ^execution.id and
              c.node_name == "answer" and
              c.computation_type == :loop and
              c.state == :success,
          order_by: [desc: c.loop_iteration],
          limit: 1
        )
        |> Journey.Repo.one()

      assert last_iter.loop_iteration == 2
      assert last_iter.loop_state == %{"disposition" => "cont_no_fallback", "value" => 2}
    end
  end

  describe "downstream nodes" do
    test "downstream compute fires when loop terminates with :ok" do
      graph =
        Journey.new_graph(
          "downstream_ok_#{random_string()}",
          "v1",
          [
            input(:seed),
            loop(
              :answer,
              [:seed],
              fn values ->
                {:ok, "seeded:#{values.seed}"}
              end,
              max_iterations: 3
            ),
            compute(:downstream, [:answer], fn %{answer: a} ->
              {:ok, "saw:#{a}"}
            end)
          ]
        )

      execution = graph |> Journey.start_execution() |> Journey.set(:seed, "x")
      assert {:ok, "saw:seeded:x", _} = Journey.get(execution, :downstream, wait: :any)
    end

    test "downstream does not fire when loop fails with :cont_no_fallback at cap" do
      graph =
        Journey.new_graph(
          "downstream_no_fire_#{random_string()}",
          "v1",
          [
            input(:seed),
            loop(
              :answer,
              [:seed],
              fn values ->
                n = values[:answer] || 0
                {:cont_no_fallback, n + 1}
              end,
              max_iterations: 2
            ),
            compute(:downstream, [:answer], fn _ -> {:ok, :should_not_fire} end)
          ]
        )

      execution = graph |> Journey.start_execution() |> Journey.set(:seed, "x")
      :timer.sleep(2_000)

      assert {:error, :not_set} = Journey.get(execution, :downstream)
    end
  end

  describe "iteration durability — error retries" do
    test "{:error, _} retries the same iteration (loop_iteration is preserved)" do
      # Use a process counter so the function can fail the first call and then succeed
      # on retry, all on iteration 1.
      counter = :counters.new(1, [])

      graph =
        single_loop_graph(
          "error_retry_#{random_string()}",
          fn _values ->
            n = :counters.get(counter, 1)
            :counters.add(counter, 1, 1)

            if n == 0 do
              {:error, "fail-first-call"}
            else
              {:ok, "succeeded-after-retry"}
            end
          end,
          max_iterations: 5,
          max_retries: 3,
          abandon_after_seconds: 5
        )

      execution = graph |> Journey.start_execution()

      assert {:ok, "succeeded-after-retry", _} = Journey.get(execution, :answer, wait: :any)

      # The retry row that succeeded was iteration 1 (not iteration 2).
      successful =
        from(c in Computation,
          where:
            c.execution_id == ^execution.id and
              c.node_name == "answer" and
              c.computation_type == :loop and
              c.state == :success
        )
        |> Journey.Repo.all()

      assert length(successful) == 1
      assert hd(successful).loop_iteration == 1
    end

    test "retry exhaustion fails the loop without promoting any value" do
      graph =
        single_loop_graph(
          "retry_exhausted_#{random_string()}",
          fn _ ->
            {:error, "always-fail"}
          end,
          max_iterations: 5,
          max_retries: 2,
          abandon_after_seconds: 5
        )

      execution = graph |> Journey.start_execution()
      :timer.sleep(2_000)

      assert {:error, :not_set} = Journey.get(execution, :answer)
    end
  end

  describe "cross-run isolation" do
    # Regression test for: a previously terminated loop run leaving :success rows
    # with non-nil loop_state must NOT pollute a subsequent run's iter 1.
    test "fresh run after upstream change sees unset self-reference on iter 1" do
      # The step function records every value of values[:answer] it observed.
      # ETS captures observations across iterations and across runs.
      table = :ets.new(:obs, [:public, :set])
      :ets.insert(table, {:obs, []})

      record = fn label ->
        :ets.update_element(
          table,
          :obs,
          {2, [label | :ets.lookup_element(table, :obs, 2)]}
        )
      end

      graph =
        Journey.new_graph(
          "cross_run_isolation_#{random_string()}",
          "v1",
          [
            input(:seed),
            loop(
              :answer,
              [:seed],
              fn values ->
                # Record what self-reference the iteration sees.
                record.({values.seed, Map.get(values, :answer)})

                case Map.get(values, :answer) do
                  nil ->
                    # First iteration of this run — start an accumulator.
                    {:cont_with_fallback, [values.seed]}

                  list when is_list(list) and length(list) >= 2 ->
                    # Done — terminate after a few continuations.
                    {:ok, list}

                  list ->
                    {:cont_with_fallback, [values.seed | list]}
                end
              end,
              max_iterations: 5
            )
          ]
        )

      execution = graph |> Journey.start_execution() |> Journey.set(:seed, "A")
      assert {:ok, ["A", "A"], rev_a} = Journey.get(execution, :answer, wait: :any)

      # Run B: change upstream. Iter 1 of Run B MUST see values.answer as nil
      # (not the last :cont_* payload from Run A and not Run A's terminal value).
      execution = Journey.set(execution, :seed, "B")
      assert {:ok, ["B", "B"], _} = Journey.get(execution, :answer, wait: {:newer_than, rev_a})

      observations = :ets.lookup_element(table, :obs, 2) |> Enum.reverse()

      # Run A: iter 1 sees nil, iter 2 sees ["A"], iter 3 sees ["A", "A"] then :ok.
      run_a_observations = [{"A", nil}, {"A", ["A"]}, {"A", ["A", "A"]}]
      assert Enum.take(observations, 3) == run_a_observations

      # Run B: iter 1 MUST also see nil (the bug being regression-tested would
      # have it see ["A", "A"] from Run A's last cont_with_fallback payload).
      run_b_observations = Enum.drop(observations, 3)
      assert hd(run_b_observations) == {"B", nil}
    end
  end

  describe "f_on_save semantics" do
    # f_on_save for :loop fires when the loop terminally resolves: {:ok, value} on terminal :ok or
    # cap-promoted :cont_with_fallback; {:error, "max_iterations_reached"} on cap-failed
    # :cont_no_fallback; {:error, reason} on retry-exhausted iteration errors. Mid-iteration :cont_*
    # and transient {:error, _} that get retried do not fire.
    test "fires once with {:ok, value} for terminal :ok" do
      callbacks = capture_table()

      graph =
        Journey.new_graph(
          "f_on_save_terminal_ok_#{random_string()}",
          "v1",
          [
            loop(
              :answer,
              [],
              fn values ->
                state = values[:answer] || 0
                if state >= 2, do: {:ok, state}, else: {:cont_with_fallback, state + 1}
              end,
              max_iterations: 5,
              f_on_save: capture_callback(callbacks)
            )
          ]
        )

      execution = graph |> Journey.start_execution()
      {:ok, 2, _} = Journey.get(execution, :answer, wait: :any)

      # Wait briefly for any deferred callbacks to fire (Task.start fire-and-forget).
      :timer.sleep(200)

      observed = :ets.lookup_element(callbacks, :obs, 2) |> Enum.reverse()
      assert observed == [{:answer, {:ok, 2}}]
    end

    test "fires once with {:ok, value} for cap-promoted :cont_with_fallback" do
      callbacks = capture_table()

      graph =
        Journey.new_graph(
          "f_on_save_cap_promote_#{random_string()}",
          "v1",
          [
            loop(
              :answer,
              [],
              fn values ->
                n = values[:answer] || 0
                {:cont_with_fallback, n + 1}
              end,
              max_iterations: 2,
              f_on_save: capture_callback(callbacks)
            )
          ]
        )

      execution = graph |> Journey.start_execution()
      {:ok, 2, _} = Journey.get(execution, :answer, wait: :any)

      :timer.sleep(200)

      # Cap-promotion delivers exactly one callback shaped as {:ok, value}, NOT
      # {:cont_with_fallback, value}.
      observed = :ets.lookup_element(callbacks, :obs, 2) |> Enum.reverse()
      assert observed == [{:answer, {:ok, 2}}]
    end

    test "fires once with {:error, \"max_iterations_reached\"} for cap-failed :cont_no_fallback" do
      callbacks = capture_table()

      graph =
        Journey.new_graph(
          "f_on_save_cap_fail_#{random_string()}",
          "v1",
          [
            loop(
              :answer,
              [],
              fn values ->
                n = values[:answer] || 0
                {:cont_no_fallback, n + 1}
              end,
              max_iterations: 2,
              f_on_save: capture_callback(callbacks)
            )
          ]
        )

      _execution = graph |> Journey.start_execution()

      # Wait for both iterations to run + the cap-failure to fire f_on_save.
      :timer.sleep(2_000)

      observed = :ets.lookup_element(callbacks, :obs, 2) |> Enum.reverse()
      assert observed == [{:answer, {:error, "max_iterations_reached"}}]
    end

    test "fires once with {:error, reason} after retry exhaustion" do
      callbacks = capture_table()

      graph =
        Journey.new_graph(
          "f_on_save_error_#{random_string()}",
          "v1",
          [
            loop(
              :answer,
              [],
              fn _ -> {:error, "always-fail"} end,
              max_iterations: 5,
              max_retries: 2,
              abandon_after_seconds: 5,
              f_on_save: capture_callback(callbacks)
            )
          ]
        )

      _execution = graph |> Journey.start_execution()
      # Allow time for both retry attempts (2 attempts × jitter up to 10s) plus async callback.
      :timer.sleep(25_000)

      observed = :ets.lookup_element(callbacks, :obs, 2) |> Enum.reverse()
      # Exactly one observation, of {:error, _} shape; reason is the inspected/truncated form.
      assert [{:answer, {:error, reason}}] = observed
      assert is_binary(reason)
      assert reason =~ "always-fail"
    end
  end

  # -- helpers -------------------------------------------------------------------

  defp single_loop_graph(graph_name, f_loop, opts) do
    Journey.new_graph(
      graph_name,
      "v1",
      [
        loop(:answer, [], f_loop, opts)
      ]
    )
  end

  defp capture_table do
    table = :ets.new(:f_on_save_obs, [:public, :set])
    :ets.insert(table, {:obs, []})
    table
  end

  defp capture_callback(table) do
    fn _execution_id, node_name, result ->
      :ets.update_element(
        table,
        :obs,
        {2, [{node_name, result} | :ets.lookup_element(table, :obs, 2)]}
      )
    end
  end
end
