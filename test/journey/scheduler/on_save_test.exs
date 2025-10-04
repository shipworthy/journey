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
