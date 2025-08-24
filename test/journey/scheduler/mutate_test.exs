defmodule Journey.Scheduler.Scheduler.MutateTest do
  use ExUnit.Case, async: true

  require Logger
  import WaitForIt
  import Journey.Node

  describe "mutate |" do
    test "useless machine says lol not so fast" do
      graph = graph_v0()
      execution = graph |> Journey.start_execution()

      assert Journey.get_value(execution, :switch_position) == {:error, :not_set}
      assert Journey.Executions.find_value_by_name(execution, :switch_position).ex_revision == 0

      execution = execution |> Journey.set_value(:switch_position, "on")
      wait_for_switch_to_be_turned_back_off(execution, 1)

      execution = execution |> Journey.set_value(:switch_position, "on")
      wait_for_switch_to_be_turned_back_off(execution, 4)

      execution = execution |> Journey.set_value(:switch_position, "on")
      wait_for_switch_to_be_turned_back_off(execution, 7)

      execution = execution |> Journey.set_value(:switch_position, "on")
      wait_for_switch_to_be_turned_back_off(execution, 10)

      Journey.Tools.summarize_as_text(execution.id) |> IO.puts()
    end
  end

  def graph_v0() do
    Journey.new_graph(
      "Useless Machine graph 0 #{__MODULE__}",
      "v1.0.0",
      [
        input(:switch_position),
        mutate(:turn_off, [:switch_position], &lol_no/1, mutates: :switch_position)
      ]
    )
  end

  def lol_no(%{switch_position: switch_position}) do
    IO.puts("switch position #{inspect(switch_position)}. useless machine says: lol not so fast")
    {:ok, "off"}
  end

  defp wait_for(f) do
    wait(f.(), timeout: 20_000, frequency: 500)
  end

  def wait_for_switch_to_be_turned_back_off(execution, expected_next_revision) do
    assert wait_for(fn ->
             execution = execution.id |> Journey.load()
             expected_next_revision == Journey.Executions.find_value_by_name(execution, :switch_position).ex_revision
           end)

    assert wait_for(fn -> {:ok, "off"} == Journey.get_value(execution, :switch_position) end)
  end
end
