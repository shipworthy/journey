defmodule Journey.Scheduler.BackgroundSweeps.ScheduleNodesTest do
  @moduledoc """
  This test module has been split into three focused modules for better parallel execution:

  - `ScheduleNodesOptimizationTest` - Long-running optimization tests with Process.sleep calls
  - `ScheduleNodesCoreTest` - Core functionality tests for get_last_sweep_cutoff/1
  - `ScheduleNodesEdgeCasesTest` - Edge cases and error handling tests

  This placeholder module remains for historical compatibility.
  """

  use ExUnit.Case, async: true

  test "module split completed successfully" do
    # This test ensures the split was completed and the new modules exist
    assert Code.ensure_loaded?(Journey.Scheduler.BackgroundSweeps.ScheduleNodesOptimizationTest)
    assert Code.ensure_loaded?(Journey.Scheduler.BackgroundSweeps.ScheduleNodesCoreTest)
    assert Code.ensure_loaded?(Journey.Scheduler.BackgroundSweeps.ScheduleNodesEdgeCasesTest)
  end
end
