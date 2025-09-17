defmodule Journey.Scheduler.Background.Sweeps.ScheduleNodesEdgeCasesTest do
  use ExUnit.Case, async: true

  alias Journey.Persistence.Schema.SweepRun
  alias Journey.Scheduler.Background.Sweeps.ScheduleNodes

  describe "edge cases and error scenarios" do
    test "handles sweep operation gracefully" do
      # Sweep should complete without error regardless of execution count
      {kicked_count, sweep_run_id} = ScheduleNodes.sweep(nil)

      # Should have processed some number of executions (might be 0 if empty)
      assert kicked_count >= 0

      # Should still record sweep run
      sweep_run = Journey.Repo.get!(SweepRun, sweep_run_id)
      assert sweep_run.sweep_type == :schedule_nodes
      assert not is_nil(sweep_run.completed_at)
    end

    test "handles non-existent execution_id parameter" do
      # Try to sweep a non-existent execution
      non_existent_id = Ecto.UUID.generate()
      {kicked_count, _sweep_run_id} = ScheduleNodes.sweep(non_existent_id)

      # Should process 0 executions
      assert kicked_count == 0
    end
  end
end
