defmodule Journey.Scheduler.Scheduler.MutateTest do
  use ExUnit.Case, async: true

  require Logger

  import Journey.Node

  describe "mutate |" do
    test "useless machine says lol not so fast" do
      graph = graph_v0()
      execution = graph |> Journey.start_execution()

      assert Journey.get_value(execution, :switch_position) == {:error, :not_set}
      assert Journey.Executions.find_value_by_name(execution, :switch_position).ex_revision == 0

      execution = execution |> Journey.set_value(:switch_position, "on")
      # TODO: allow for get_value(..., wait: {revision_exceeds: 1}) or some such, instead of sleeping.
      Process.sleep(2000)
      execution = execution |> Journey.load()
      assert Journey.Executions.find_value_by_name(execution, :switch_position).ex_revision == 1
      assert Journey.get_value(execution, :switch_position, wait: true) == {:ok, "off"}

      execution = execution |> Journey.set_value(:switch_position, "on")
      Process.sleep(2000)
      execution = execution |> Journey.load()
      assert Journey.Executions.find_value_by_name(execution, :switch_position).ex_revision == 4
      assert Journey.get_value(execution, :switch_position, wait: true) == {:ok, "off"}

      execution = execution |> Journey.set_value(:switch_position, "on")
      Process.sleep(2000)
      execution = execution |> Journey.load()
      assert Journey.Executions.find_value_by_name(execution, :switch_position).ex_revision == 7
      assert Journey.get_value(execution, :switch_position, wait: true) == {:ok, "off"}

      execution = execution |> Journey.set_value(:switch_position, "on")
      Process.sleep(2000)
      execution = execution |> Journey.load()
      assert Journey.Executions.find_value_by_name(execution, :switch_position).ex_revision == 10
      assert Journey.get_value(execution, :switch_position, wait: true) == {:ok, "off"}

      Journey.Tools.summarize(execution.id) |> IO.puts()
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
end
