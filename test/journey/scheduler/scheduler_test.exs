defmodule Journey.Scheduler.SchedulerTest do
  use ExUnit.Case, async: true

  import Journey
  import Journey.Helpers.GrabBag

  alias Journey.Scheduler

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
    @tag :capture_log
    test "none" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)

      assert [] = Scheduler.sweep_abandoned_computations(execution.id)
      execution = Journey.set_value(execution, :birth_month, "April")

      assert Journey.values(execution) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set
             }

      assert Scheduler.sweep_abandoned_computations(execution.id) == []

      assert Journey.values(execution) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set
             }
    end

    @tag :capture_log
    test "one execution" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")

      assert [] = Scheduler.sweep_abandoned_computations(execution.id)

      Process.sleep(2_000)

      [abandoned_computation] = Scheduler.sweep_abandoned_computations(execution.id)
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

      assert [] = Scheduler.sweep_abandoned_computations(execution.id)

      Process.sleep(2_000)

      [abandoned_computation] = Scheduler.sweep_abandoned_computations(nil)

      assert abandoned_computation.state == :abandoned
      assert abandoned_computation.computation_type == :compute
      assert abandoned_computation.node_name == :astrological_sign
      assert abandoned_computation.execution_id == execution.id
    end

    @tag :capture_log
    test "background sweeps with retries" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")

      assert Journey.values(execution) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set
             }

      Process.sleep(2_000)
      swept_ids = Scheduler.BackgroundSweep.find_and_kickoff_abandoned_computations() |> ids_of()
      assert execution.id in swept_ids
      assert 1 == count_computations(execution.id, :astrological_sign, :abandoned)
      assert 1 == count_computations(execution.id, :astrological_sign, :computing)
      Process.sleep(2_000)
      swept_ids = Scheduler.BackgroundSweep.find_and_kickoff_abandoned_computations() |> ids_of()
      assert execution.id in swept_ids
      assert 2 == count_computations(execution.id, :astrological_sign, :abandoned)
      assert 0 == count_computations(execution.id, :astrological_sign, :computing)
      Process.sleep(2_000)
      swept_ids = Scheduler.BackgroundSweep.find_and_kickoff_abandoned_computations() |> ids_of()
      assert execution.id not in swept_ids
      Process.sleep(2_000)
      swept_ids = Scheduler.BackgroundSweep.find_and_kickoff_abandoned_computations() |> ids_of()
      assert execution.id not in swept_ids
      Process.sleep(2_000)
      assert 2 == count_computations(execution.id, :astrological_sign, :abandoned)
      assert 0 == count_computations(execution.id, :astrological_sign, :computing)

      assert Journey.values(execution) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set
             }
    end

    defp count_computations(execution_id, node_atom, state_atom) do
      execution_id
      |> Journey.load()
      |> Map.get(:computations)
      |> Enum.count(fn c -> c.node_name == node_atom and c.state == state_atom end)
    end

    @tag :capture_log
    test "background sweep" do
      execution_ids =
        for _ <- 1..10 do
          create_graph()
          |> Journey.start_execution()
          |> Journey.set_value(:birth_day, 26)
          |> Journey.set_value(:birth_month, "April")
        end
        |> ids_of()
        |> MapSet.new()

      Process.sleep(2_000)

      all_swept =
        Scheduler.BackgroundSweep.find_and_kickoff_abandoned_computations()
        |> ids_of()
        |> MapSet.new()

      assert MapSet.subset?(execution_ids, all_swept)
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
            Process.sleep(:timer.seconds(5))
            {:ok, "Taurus"}
          end,
          abandon_after_seconds: 1,
          max_retries: 2
        )
      ],
      []
    )
  end
end
