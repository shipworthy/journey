defmodule Journey.Scheduler.SchedulerTest do
  use ExUnit.Case, async: true

  import Journey

  describe "advance |" do
    test "no executable steps" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)

      updated_execution = Journey.Scheduler.advance(execution)
      assert updated_execution == execution
    end
  end

  describe "sweep_abandoned_computations |" do
    test "none" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)

      assert [] = Journey.Scheduler.sweep_abandoned_computations(execution.id)
      execution = Journey.set_value(execution, :birth_month, "April")

      assert Journey.values(execution) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set
             }

      assert Journey.Scheduler.sweep_abandoned_computations(execution.id) == []

      assert Journey.values(execution) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set
             }
    end

    test "one execution" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")

      assert [] = Journey.Scheduler.sweep_abandoned_computations(execution.id)

      Process.sleep(2_000)

      [abandoned_computation] = Journey.Scheduler.sweep_abandoned_computations(execution.id)
      assert abandoned_computation.state == :abandoned
      assert abandoned_computation.computation_type == :compute
      assert abandoned_computation.node_name == :astrological_sign
      assert abandoned_computation.execution_id == execution.id
    end

    # TODO: run this in a sql sandbox
    @tag :skip
    test "system-wide" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")

      assert [] = Journey.Scheduler.sweep_abandoned_computations(execution.id)

      Process.sleep(2_000)

      [abandoned_computation] = Journey.Scheduler.sweep_abandoned_computations(nil)

      assert abandoned_computation.state == :abandoned
      assert abandoned_computation.computation_type == :compute
      assert abandoned_computation.node_name == :astrological_sign
      assert abandoned_computation.execution_id == execution.id
    end

    test "background sweep" do
      for _ <- 1..10 do
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")
      end

      Process.sleep(2_000)

      Journey.Scheduler.BackgroundSweep.find_and_kickoff_abandoned_computations()
    end
  end

  defp create_graph() do
    Journey.new_graph(
      "astrological sign workflow, abandoned compute #{__MODULE__}",
      "v1.0.0",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(
          :astrological_sign,
          [:birth_month, :birth_day],
          fn %{birth_month: _birth_month, birth_day: _birth_day} ->
            Process.sleep(:timer.seconds(8))
            {:ok, "Taurus"}
          end,
          abandon_after_seconds: 1
        )
      ],
      []
    )
  end
end
