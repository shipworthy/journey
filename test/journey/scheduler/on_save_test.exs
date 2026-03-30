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
