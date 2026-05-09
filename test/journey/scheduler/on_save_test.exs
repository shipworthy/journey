defmodule Journey.Scheduler.Scheduler.OnSaveTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  import Journey.Node

  require Logger

  test "on save basic test" do
    graph = simple_graph()
    execution = graph |> Journey.start_execution()

    {_result, log} =
      with_log(fn ->
        execution = execution |> Journey.set(:user_name, "Mario")
        {:ok, "Hello, Mario", _} = Journey.get(execution, :greeting, wait: :any)
        Process.sleep(2000)
      end)

    assert log =~ "f_on_save: Hello, {:ok, \"Hello, Mario\"}"
  end

  test "3-arity node-specific f_on_save receives node name" do
    test_pid = self()

    graph =
      Journey.new_graph(
        "3-arity f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:user_name),
          compute(
            :greeting,
            [:user_name],
            fn %{user_name: user_name} ->
              {:ok, "Hello, #{user_name}"}
            end,
            f_on_save: fn execution_id, node_name, result ->
              send(test_pid, {:node_on_save, execution_id, node_name, result})
              :ok
            end
          )
        ]
      )

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :user_name, "Mario")
    {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait_any: true)

    assert_receive {:node_on_save, exec_id, :greeting, {:ok, "Hello, Mario"}}, 3000
    assert exec_id == execution.id
  end

  test "2-arity and 3-arity f_on_save coexist in same graph" do
    test_pid = self()

    graph =
      Journey.new_graph(
        "mixed arity f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:value),
          compute(
            :double,
            [:value],
            fn %{value: v} -> {:ok, v * 2} end,
            f_on_save: fn _execution_id, result ->
              send(test_pid, {:arity2_callback, result})
              :ok
            end
          ),
          compute(
            :triple,
            [:value],
            fn %{value: v} -> {:ok, v * 3} end,
            f_on_save: fn _execution_id, node_name, result ->
              send(test_pid, {:arity3_callback, node_name, result})
              :ok
            end
          )
        ]
      )

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :value, 5)

    {:ok, 10} = Journey.get_value(execution, :double, wait_any: true)
    {:ok, 15} = Journey.get_value(execution, :triple, wait_any: true)

    assert_receive {:arity2_callback, {:ok, 10}}, 3000
    assert_receive {:arity3_callback, :triple, {:ok, 15}}, 3000
  end

  test "node-specific f_on_save/3 on input node" do
    test_pid = self()

    graph =
      Journey.new_graph(
        "input f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:name,
            f_on_save: fn execution_id, node_name, result ->
              send(test_pid, {:input_on_save, execution_id, node_name, result})
              :ok
            end
          )
        ]
      )

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :name, "Mario")

    assert_receive {:input_on_save, exec_id, :name, {:ok, "Mario"}}, 3000
    assert exec_id == execution.id
  end

  test "graph-wide f_on_save fires for input node" do
    test_pid = self()

    graph =
      Journey.new_graph(
        "graph-wide input f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:name)
        ],
        f_on_save: fn execution_id, node_name, result ->
          send(test_pid, {:graph_on_save, execution_id, node_name, result})
          :ok
        end
      )

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :name, "Luigi")

    assert_receive {:graph_on_save, exec_id, :name, {:ok, "Luigi"}}, 3000
    assert exec_id == execution.id
  end

  test "both node-specific and graph-wide f_on_save fire for input node" do
    test_pid = self()

    graph =
      Journey.new_graph(
        "both f_on_save input #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:name,
            f_on_save: fn execution_id, node_name, result ->
              send(test_pid, {:node_callback, execution_id, node_name, result})
              :ok
            end
          )
        ],
        f_on_save: fn execution_id, node_name, result ->
          send(test_pid, {:graph_callback, execution_id, node_name, result})
          :ok
        end
      )

    execution = Journey.start_execution(graph)
    Journey.set(execution, :name, "Peach")

    assert_receive {:node_callback, _, :name, {:ok, "Peach"}}, 3000
    assert_receive {:graph_callback, _, :name, {:ok, "Peach"}}, 3000
  end

  test "multi-value set fires f_on_save once per changed node" do
    test_pid = self()

    make_callback = fn tag ->
      fn _execution_id, node_name, result ->
        send(test_pid, {tag, node_name, result})
        :ok
      end
    end

    graph =
      Journey.new_graph(
        "multi-value f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:first, f_on_save: make_callback.(:cb)),
          input(:second, f_on_save: make_callback.(:cb))
        ]
      )

    execution = Journey.start_execution(graph)
    Journey.set(execution, %{first: "a", second: "b"})

    assert_receive {:cb, :first, {:ok, "a"}}, 3000
    assert_receive {:cb, :second, {:ok, "b"}}, 3000
  end

  test "multi-value set only fires f_on_save for changed nodes" do
    test_pid = self()

    make_callback = fn ->
      fn _execution_id, node_name, result ->
        send(test_pid, {:cb, node_name, result})
        :ok
      end
    end

    graph =
      Journey.new_graph(
        "multi-value changed-only f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:first, f_on_save: make_callback.()),
          input(:second, f_on_save: make_callback.())
        ]
      )

    execution = Journey.start_execution(graph)

    # Set :first to "a"
    execution = Journey.set(execution, :first, "a")
    assert_receive {:cb, :first, {:ok, "a"}}, 3000

    # Now set both, but :first is unchanged
    Journey.set(execution, %{first: "a", second: "b"})

    assert_receive {:cb, :second, {:ok, "b"}}, 3000
    # :first should NOT fire since its value didn't change
    refute_receive {:cb, :first, _}, 1000
  end

  test "f_on_save does not fire when input value is unchanged" do
    test_pid = self()

    graph =
      Journey.new_graph(
        "no-change f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:name,
            f_on_save: fn _execution_id, node_name, result ->
              send(test_pid, {:cb, node_name, result})
              :ok
            end
          )
        ]
      )

    execution = Journey.start_execution(graph)

    # First set fires
    execution = Journey.set(execution, :name, "Mario")
    assert_receive {:cb, :name, {:ok, "Mario"}}, 3000

    # Same value again — should NOT fire
    Journey.set(execution, :name, "Mario")
    refute_receive {:cb, :name, _}, 1000
  end

  test "error in input f_on_save does not break set/3" do
    graph =
      Journey.new_graph(
        "error f_on_save #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:name,
            f_on_save: fn _execution_id, _node_name, _result ->
              raise "intentional error in f_on_save"
            end
          )
        ]
      )

    execution = Journey.start_execution(graph)
    # Should not raise — f_on_save errors are caught
    execution = Journey.set(execution, :name, "Mario")
    assert %{name: "Mario"} = Journey.values(execution)
  end

  test "input/1 backward compatibility — f_on_save defaults to nil" do
    graph =
      Journey.new_graph(
        "input/1 compat #{Journey.Helpers.Random.random_string()}",
        "1.0.0",
        [
          input(:name)
        ]
      )

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :name, "Mario")
    assert %{name: "Mario"} = Journey.values(execution)
  end

  describe "compute terminal-failure semantics" do
    @tag :capture_log
    test "compute fires once with {:error, reason} after retry exhaustion" do
      test_pid = self()

      graph =
        Journey.new_graph(
          "compute retry exhaustion #{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [
            input(:trigger),
            compute(
              :always_fails,
              [:trigger],
              fn _ -> {:error, "boom"} end,
              # max_retries: 0 means: 1 attempt total, then exhaustion (count=1, 1 < 0 is false).
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

      # The {:error, _} clause in handle_computation_result sleeps with up to 10s of jitter.
      # Allow plenty of room for the single attempt + jitter + async callback.
      assert_receive {:cb, :always_fails, {:error, reason}}, 15_000
      assert is_binary(reason)
      assert reason =~ "boom"

      # No second callback should arrive.
      refute_receive {:cb, :always_fails, _}, 1_000
    end

    test "compute is silent on transient errors — fires once with {:ok, _} on eventual success" do
      test_pid = self()
      counter = :counters.new(1, [])

      graph =
        Journey.new_graph(
          "compute transient then success #{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [
            input(:trigger),
            compute(
              :flaky,
              [:trigger],
              fn _ ->
                :counters.add(counter, 1, 1)
                attempt = :counters.get(counter, 1)

                if attempt >= 2 do
                  {:ok, "succeeded on attempt #{attempt}"}
                else
                  {:error, "transient on attempt #{attempt}"}
                end
              end,
              max_retries: 3,
              f_on_save: fn _execution_id, node_name, result ->
                send(test_pid, {:cb, node_name, result})
                :ok
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      Journey.set(execution, :trigger, "go")

      # The transient {:error, _} on the first attempt should NOT fire f_on_save under the new rule.
      # Only the eventual {:ok, _} fires. Allow generous time: jitter sleep up to 10s + 2nd attempt.
      assert_receive {:cb, :flaky, {:ok, value}}, 20_000
      assert value =~ "succeeded"

      # Confirm the transient error did not fire.
      refute_receive {:cb, :flaky, {:error, _}}, 500
    end

    test "compute is silent on idempotent no-change re-runs" do
      test_pid = self()

      # f_compute returns a constant regardless of upstream — so a recompute triggered by an upstream
      # change produces an unchanged output, and record_result returns :no_value_written.
      graph =
        Journey.new_graph(
          "compute idempotent re-run #{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [
            input(:trigger),
            compute(
              :constant,
              [:trigger],
              fn _ -> {:ok, "always the same"} end,
              f_on_save: fn _execution_id, node_name, result ->
                send(test_pid, {:cb, node_name, result})
                :ok
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :trigger, "first")

      # First computation writes the value — fires.
      assert_receive {:cb, :constant, {:ok, "always the same"}}, 3_000

      # Change upstream to trigger recompute. Output is unchanged, so :no_value_written → silent.
      Journey.set(execution, :trigger, "second")
      refute_receive {:cb, :constant, _}, 2_000
    end
  end

  defp simple_graph() do
    Journey.new_graph(
      "simple graph #{__MODULE__}",
      "1.0.0",
      [
        input(:user_name),
        compute(
          :greeting,
          [:user_name, :user_name],
          fn %{user_name: user_name} ->
            {:ok, "Hello, #{user_name}"}
          end,
          f_on_save: fn _execution_id, params ->
            Logger.error("f_on_save: Hello, #{inspect(params)}")
            :ok
          end
        )
      ]
    )
  end
end
