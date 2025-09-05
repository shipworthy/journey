defmodule Journey.Scheduler.Background.Sweeps.StalledExecutionsTest do
  use ExUnit.Case, async: false
  import Ecto.Query
  import Journey.Node
  import Journey.Helpers.Random

  alias Journey.Persistence.Schema.Execution
  alias Journey.Persistence.Schema.Execution.Value
  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.StalledExecutions

  setup do
    # Clean slate - delete all sweep runs for this test
    Journey.Repo.delete_all(from(sr in SweepRun, where: sr.sweep_type == :stalled_executions))
    :ok
  end

  describe "basic functionality" do
    test "returns {0, nil} when disabled" do
      original_config = Application.get_env(:journey, :stalled_executions_sweep, [])

      Application.put_env(:journey, :stalled_executions_sweep, enabled: false)

      try do
        {count, sweep_run_id} = StalledExecutions.sweep()
        assert count == 0
        assert sweep_run_id == nil
      after
        Application.put_env(:journey, :stalled_executions_sweep, original_config)
      end
    end

    test "returns {0, nil} when wrong hour" do
      current_hour = DateTime.utc_now().hour
      different_hour = rem(current_hour + 3, 24)

      original_config = Application.get_env(:journey, :stalled_executions_sweep, [])

      Application.put_env(:journey, :stalled_executions_sweep, preferred_hour: different_hour)

      try do
        {count, sweep_run_id} = StalledExecutions.sweep()
        assert count == 0
        assert sweep_run_id == nil
      after
        Application.put_env(:journey, :stalled_executions_sweep, original_config)
      end
    end

    test "creates sweep run when enabled with no hour restriction" do
      original_config = Application.get_env(:journey, :stalled_executions_sweep, [])

      Application.put_env(:journey, :stalled_executions_sweep,
        enabled: true,
        preferred_hour: nil
      )

      try do
        # Use a non-existent execution_id to avoid processing historical executions
        {count, sweep_run_id} = StalledExecutions.sweep("NON_EXISTENT_ID")
        assert sweep_run_id != nil
        assert count == 0

        # Verify sweep run was created
        sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
        assert sweep_run.sweep_type == :stalled_executions
        assert sweep_run.started_at != nil
        assert sweep_run.completed_at != nil
        assert sweep_run.executions_processed == count
      after
        Application.put_env(:journey, :stalled_executions_sweep, original_config)
      end
    end

    test "respects 23-hour minimum between runs" do
      original_config = Application.get_env(:journey, :stalled_executions_sweep, [])

      Application.put_env(:journey, :stalled_executions_sweep,
        enabled: true,
        preferred_hour: nil
      )

      try do
        # Create first sweep run with non-existent ID to avoid processing historical executions
        {_count1, sweep_run_id1} = StalledExecutions.sweep("NON_EXISTENT_ID_1")
        assert sweep_run_id1 != nil

        # Try to run again immediately - should be blocked
        {count2, sweep_run_id2} = StalledExecutions.sweep("NON_EXISTENT_ID_2")
        assert count2 == 0
        assert sweep_run_id2 == nil
      after
        Application.put_env(:journey, :stalled_executions_sweep, original_config)
      end
    end

    test "handles non-existent execution_id gracefully" do
      original_config = Application.get_env(:journey, :stalled_executions_sweep, [])

      Application.put_env(:journey, :stalled_executions_sweep,
        enabled: true,
        preferred_hour: nil
      )

      try do
        {count, _sweep_run_id} = StalledExecutions.sweep("DOES_NOT_EXIST")
        assert count == 0
      after
        Application.put_env(:journey, :stalled_executions_sweep, original_config)
      end
    end

    test "sweep actually computes stalled execution" do
      unique_id = random_string()
      original_config = Application.get_env(:journey, :stalled_executions_sweep, [])

      Application.put_env(:journey, :stalled_executions_sweep,
        enabled: true,
        preferred_hour: nil
      )

      try do
        # Create execution
        graph =
          Journey.new_graph(
            "stalled-compute-test-#{unique_id}",
            "v1.0.0",
            [
              input(:input_value),
              compute(:computed_result, [:input_value], fn %{input_value: v} -> {:ok, "computed: #{v}"} end)
            ]
          )

        execution = Journey.start_execution(graph)

        # Set input value but don't trigger computation (simulate stalled state)
        valid_time = System.system_time(:second) - 30 * 60

        from(v in Value, where: v.execution_id == ^execution.id and v.node_name == "input_value")
        |> Journey.Repo.update_all(
          set: [
            node_value: "test_input",
            set_time: valid_time,
            ex_revision: 1,
            updated_at: System.system_time(:second)
          ]
        )

        from(e in Execution, where: e.id == ^execution.id)
        |> Journey.Repo.update_all(set: [updated_at: valid_time, revision: 1])

        # Verify initial state - input set but compute not completed
        execution = Journey.load(execution.id)
        values = Journey.values(execution)
        assert values[:input_value] == "test_input"
        assert values[:computed_result] == nil

        # Run sweep on this specific execution
        {count, sweep_run_id} = StalledExecutions.sweep(execution.id)
        assert count == 1
        assert sweep_run_id != nil

        # Wait for computation to complete using wait_any
        result = Journey.get_value(execution, :computed_result, wait_any: true)
        assert result == {:ok, "computed: test_input"}
      after
        Application.put_env(:journey, :stalled_executions_sweep, original_config)
      end
    end
  end
end
