defmodule Journey.Scheduler.SchedulerTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Helpers.GrabBag

  alias Journey.Scheduler
  alias Journey.Scheduler.BackgroundSweeps.Abandoned

  describe "advance |" do
    test "no executable steps" do
      execution =
        create_graph(:success)
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)

      updated_execution = Scheduler.advance(execution)
      assert updated_execution == execution
    end

    @tag :capture_log
    test "retries on failures" do
      execution =
        create_graph(:failure)
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")

      # TODO: replace sleep with get_value(execution, :astrological_sign, wait: true)
      Process.sleep(11_000)
      assert 2 == count_computations(execution.id, :astrological_sign, :failed)
      assert 0 == count_computations(execution.id, :astrological_sign, :computing)
    end
  end

  describe "find_and_maybe_reschedule |" do
    @tag :capture_log
    test "none" do
      execution =
        create_graph(:timeout)
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)

      assert [] = Abandoned.find_and_maybe_reschedule(execution.id)
      execution = Journey.set_value(execution, :birth_month, "April")

      assert Journey.values_all(execution) |> Map.update!(:execution_id, fn _ -> {:set, "EXEC..."} end) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set,
               execution_id: {:set, "EXEC..."}
             }

      assert Abandoned.find_and_maybe_reschedule(execution.id) == []

      assert Journey.values_all(execution) |> Map.update!(:execution_id, fn _ -> {:set, "EXEC..."} end) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set,
               execution_id: {:set, "EXEC..."}
             }
    end

    @tag :capture_log
    test "one execution, computation abandoned" do
      execution =
        create_graph(:timeout)
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")

      assert [] = Abandoned.find_and_maybe_reschedule(execution.id)
      assert 1 == count_computations(execution.id, :astrological_sign, :computing)

      # After a wait, the next sweep identifies the computation as :abandoned.
      Process.sleep(2_000)
      [abandoned_computation] = Abandoned.find_and_maybe_reschedule(execution.id)
      assert abandoned_computation.state == :abandoned
      assert abandoned_computation.computation_type == :compute
      assert abandoned_computation.node_name == :astrological_sign
      assert abandoned_computation.execution_id == execution.id

      assert execution |> Journey.load() |> Map.get(:computations) |> Enum.count() == 2

      # Give the node's f_compute the time it needs to complete.
      # The abandoned computation should remain :abandoned.
      Process.sleep(5_000)
      current_computations = execution |> Journey.load() |> Map.get(:computations)
      assert length(current_computations) == 2
      assert Enum.find(current_computations, fn %{id: id} -> id == abandoned_computation.id end).state == :abandoned
    end

    # TODO: run this in a sql sandbox
    @tag :skip
    test "system-wide" do
      execution =
        create_graph(:timeout)
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")

      assert [] = Abandoned.find_and_maybe_reschedule(execution.id)

      Process.sleep(2_000)

      [abandoned_computation] = Abandoned.find_and_maybe_reschedule(nil)

      assert abandoned_computation.state == :abandoned
      assert abandoned_computation.computation_type == :compute
      assert abandoned_computation.node_name == :astrological_sign
      assert abandoned_computation.execution_id == execution.id
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
          create_graph(:timeout)
          |> Journey.start_execution()
          |> Journey.set_value(:birth_day, 26)
          |> Journey.set_value(:birth_month, "April")
        end
        |> ids_of()
        |> MapSet.new()

      for eid <- execution_ids do
        assert [] == Abandoned.sweep(eid)
      end

      Process.sleep(2_000)

      for eid <- execution_ids do
        [execution] = Abandoned.sweep(eid)
        assert execution.id == eid
      end
    end
  end

  defp create_graph(behavior) when behavior in [:success, :failure, :timeout] do
    Journey.new_graph(
      "astrological sign workflow, #{behavior} compute #{__MODULE__}",
      "v2.0.0",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(
          :astrological_sign,
          [:birth_month, :birth_day],
          fn %{birth_month: _birth_month, birth_day: _birth_day} ->
            case behavior do
              :timeout ->
                Process.sleep(:timer.seconds(5))
                {:ok, "Taurus"}

              :success ->
                {:ok, "Taurus"}

              :failure ->
                {:error, "simulated failure"}
            end
          end,
          abandon_after_seconds: 1,
          max_retries: 2
        )
      ]
    )
  end
end
