defmodule JourneyTestDataOnly do
  use ExUnit.Case
  doctest Journey
  doctest Journey.Process

  @tiny_process %Journey.Process{
    name: "tiny test process",
    version: "1.1.0",
    steps: [
      %Journey.Step{name: :first_name},
      %Journey.Step{name: :birth_month},
      %Journey.Step{name: :birth_day}
    ]
  }

  @tag :skip
  test "basic data-only process" do
    process = @tiny_process
    execution = Journey.Process.execute(process)
    execution = Journey.ExecutionStore.get(execution.execution_id)

    # The process should be blank, nothing is computed.
    {:not_computed, _} = Journey.Execution.read_value(execution, :first_name)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_month)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_day)
    3 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()

    # Submit first name.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :first_name, "Bobby")
    {:computed, "Bobby"} = Journey.Execution.read_value(execution, :first_name)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_month)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_day)
    2 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()

    # Submit birth month.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :birth_month, 10)
    {:computed, "Bobby"} = Journey.Execution.read_value(execution, :first_name)
    {:computed, 10} = Journey.Execution.read_value(execution, :birth_month)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_day)
    1 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()

    # Submit birth day.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :birth_day, 22)
    {:computed, "Bobby"} = Journey.Execution.read_value(execution, :first_name)
    {:computed, 10} = Journey.Execution.read_value(execution, :birth_month)
    {:computed, 22} = Journey.Execution.read_value(execution, :birth_day)
    0 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()

    # Change birth month.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :birth_month, 12)
    {:computed, "Bobby"} = Journey.Execution.read_value(execution, :first_name)
    {:computed, 12} = Journey.Execution.read_value(execution, :birth_month)
    {:computed, 22} = Journey.Execution.read_value(execution, :birth_day)
    0 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()

    # Change birth month back.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :birth_month, 10)
    {:computed, "Bobby"} = Journey.Execution.read_value(execution, :first_name)
    {:computed, 10} = Journey.Execution.read_value(execution, :birth_month)
    {:computed, 22} = Journey.Execution.read_value(execution, :birth_day)
    0 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()
  end

  @tag :skip
  test "unknown execution_id, unknown step name" do
    process = @tiny_process
    execution = Journey.Process.execute(process)
    execution = Journey.ExecutionStore.get(execution.execution_id)

    # The process should be blank, nothing is computed.
    {:not_computed, _} = Journey.Execution.read_value(execution, :first_name)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_month)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_day)
    3 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()

    # Submit first name.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :first_name, "Bobby")
    {:computed, "Bobby"} = Journey.Execution.read_value(execution, :first_name)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_month)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_day)
    2 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()

    # Submit birth month, for an unknown execution id.
    {:unknown_execution_id, _} = Journey.Execution.update_value("no such execution", :birth_month, 4)

    # Submit a value for an unknown step.
    {:unknown_step, _} = Journey.Execution.update_value(execution.execution_id, :no_such_step, 5)

    # The execution should remain unchanged.
    {:computed, "Bobby"} = Journey.Execution.read_value(execution, :first_name)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_month)
    {:not_computed, _} = Journey.Execution.read_value(execution, :birth_day)
    2 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()
  end
end
