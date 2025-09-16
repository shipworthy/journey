defmodule Journey.Scheduler.SchedulerTest do
  use ExUnit.Case, async: false

  import Journey.Node
  import Journey.Helpers.GrabBag
  import Journey.Helpers.Random
  import Ecto.Query

  alias Journey.Scheduler

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  alias Journey.Scheduler.Background.Sweeps.Abandoned

  describe "advance |" do
    test "no executable steps" do
      execution =
        create_graph(:success)
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)

      updated_execution = Scheduler.advance(execution)
      assert updated_execution == execution
    end

    @tag :capture_log
    test "retries on failures" do
      execution =
        create_graph(:failure)
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)
        |> Journey.set(:birth_month, "April")

      {:error, :computation_failed} =
        Journey.get_value(execution, :astrological_sign, wait_any: true)

      assert 2 == count_computations(execution.id, :astrological_sign, :failed)
      assert 0 == count_computations(execution.id, :astrological_sign, :computing)
    end

    @tag :capture_log
    test "retries on failures with wait_new" do
      execution =
        create_graph(:failure)
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)
        |> Journey.set(:birth_month, "April")

      # Should return immediately once retries exhausted, even with wait_new
      {:error, :computation_failed} =
        Journey.get_value(execution, :astrological_sign, wait_new: true)

      assert 2 == count_computations(execution.id, :astrological_sign, :failed)
      assert 0 == count_computations(execution.id, :astrological_sign, :computing)
    end

    @tag :capture_log
    test "retries on failures without wait options" do
      execution =
        create_graph(:failure)
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)
        |> Journey.set(:birth_month, "April")

      # Start background sweeps to enable computations
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Wait for retries to exhaust by using wait_any first
      {:error, :computation_failed} = Journey.get_value(execution, :astrological_sign, wait_any: true)

      # Should return :computation_failed immediately when called without wait options
      {:error, :computation_failed} = Journey.get_value(execution, :astrological_sign)

      stop_background_sweeps_in_test(background_sweeps_task)

      assert 2 == count_computations(execution.id, :astrological_sign, :failed)
      assert 0 == count_computations(execution.id, :astrological_sign, :computing)
    end
  end

  describe "abandoned sweep |" do
    @tag :capture_log
    test "none" do
      execution =
        create_graph(:timeout)
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)

      assert 0 == Abandoned.sweep(execution.id)
      execution = Journey.set(execution, :birth_month, "April")

      assert Journey.values_all(execution) |> redact([:execution_id, :last_updated_at]) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set,
               execution_id: {:set, "..."},
               last_updated_at: {:set, 1_234_567_890}
             }

      assert Abandoned.sweep(execution.id) == 0

      assert Journey.values_all(execution) |> redact([:execution_id, :last_updated_at]) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: :not_set,
               execution_id: {:set, "..."},
               last_updated_at: {:set, 1_234_567_890}
             }
    end

    @tag :capture_log
    test "one execution, computation abandoned" do
      execution =
        create_graph(:timeout)
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)
        |> Journey.set(:birth_month, "April")

      assert 0 == Abandoned.sweep(execution.id)
      assert 1 == count_computations(execution.id, :astrological_sign, :computing)

      # After a wait, the next sweep identifies the computation as :abandoned.
      Process.sleep(2_000)
      assert 1 == Abandoned.sweep(execution.id)

      # Verify the computation was actually abandoned
      assert 1 == count_computations(execution.id, :astrological_sign, :abandoned)

      assert execution |> Journey.load() |> Map.get(:computations) |> Enum.count() == 2

      # Give the node's f_compute the time it needs to complete.
      # The abandoned computation should remain :abandoned.
      Process.sleep(5_000)
      # Verify the abandoned computation remains abandoned
      assert 1 == count_computations(execution.id, :astrological_sign, :abandoned)
    end

    @tag :skip
    test "system-wide" do
      execution =
        create_graph(:timeout)
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)
        |> Journey.set(:birth_month, "April")

      assert 0 == Abandoned.sweep(execution.id)

      Process.sleep(2_000)

      assert 1 == Abandoned.sweep(nil)
      # Verify the computation was actually abandoned
      assert 1 == count_computations(execution.id, :astrological_sign, :abandoned)
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
          |> Journey.set(:birth_day, 26)
          |> Journey.set(:birth_month, "April")
        end
        |> ids_of()
        |> MapSet.new()

      for eid <- execution_ids do
        assert 0 == Abandoned.sweep(eid)
      end

      Process.sleep(2_000)

      for eid <- execution_ids do
        assert 1 == Abandoned.sweep(eid)
      end
    end

    test "processes multiple batches of abandoned computations" do
      # Create a simple graph with a single slow compute node
      # We'll manually create 150 computation records for it
      graph_name = "batch_test_#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "v1.0.0",
          [
            input(:name),
            compute(
              :greeting,
              [:name],
              fn %{name: name} ->
                Process.sleep(100_000)
                {:ok, "hello #{name}"}
              end,
              abandon_after_seconds: 3
            )
          ]
        )

      Journey.Graph.Catalog.list() |> Enum.map(fn g -> g.name end)

      count = 120

      execution_ids =
        for i <- 1..count do
          Journey.start_execution(graph)
          |> Journey.set(:name, "Mario #{i}")
        end
        |> Enum.map(fn e -> e.id end)

      Process.sleep(5_000)

      Journey.Repo.all(
        from c in Journey.Persistence.Schema.Execution.Computation,
          where: c.execution_id in ^execution_ids
      )

      # Run sweep with current time - should process all 150 in 2 batches
      kicked_count = Abandoned.sweep(nil)

      # Should have kicked the execution at least once (may be more due to real computations)
      assert kicked_count >= count

      # Verify all 150 computations are marked as abandoned
      abandoned_computations =
        Journey.Repo.all(
          from c in Journey.Persistence.Schema.Execution.Computation,
            where:
              c.execution_id in ^execution_ids and
                c.state == :abandoned
        )

      assert length(abandoned_computations) == count
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
