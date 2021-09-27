defmodule Journey.Test.Lifetime do
  use ExUnit.Case

  require WaitForIt

  @process %Journey.Process{
    process_id: "test process",
    steps: [
      %Journey.Step{name: :first_name},
      %Journey.Step{name: :birth_month},
      %Journey.Step{name: :birth_day},
      %Journey.Step{
        name: :astrological_sign,
        func: &Journey.Test.Lifetime.compute_astrological_sign/1,
        blocked_by: [
          %Journey.BlockedBy{step_name: :first_name, condition: :provided},
          %Journey.BlockedBy{step_name: :birth_month, condition: :provided},
          %Journey.BlockedBy{step_name: :birth_day, condition: :provided}
        ]
      },
      %Journey.Step{
        name: :extra_something_for_taurus,
        func: &Journey.Test.Lifetime.compute_taurus_bonus/1,
        blocked_by: [
          %Journey.BlockedBy{
            step_name: :astrological_sign,
            condition: %Journey.ValueCondition{condition: :value, value: :taurus}
          }
        ]
      },
      %Journey.Step{
        name: :horoscope,
        func: &Journey.Test.Lifetime.compute_horoscope/1,
        blocked_by: [
          %Journey.BlockedBy{step_name: :astrological_sign, condition: :provided}
        ]
      }
    ]
  }

  def compute_astrological_sign(_values) do
    :timer.sleep(150)
    {:ok, :taurus}
  end

  def compute_taurus_bonus(_values) do
    :timer.sleep(150)
    {:ok, "You are a taurus! Yay!"}
  end

  def compute_horoscope(_values) do
    :timer.sleep(250)
    {:ok, "You ain't got no... many things, but you got life."}
  end

  # TODO: add a test for a failing task.
  test "execute a basic process" do
    # Start process execution.
    execution = Journey.Process.execute(@process)

    6 = Journey.Execution.get_unfilled_steps(execution) |> Enum.count()
    3 = Journey.Execution.get_blocked_steps(execution) |> Enum.count()
    # Journey.Execution.get_summary(execution) |> IO.puts()
    all_available_steps = Journey.Execution.get_ordered_steps(execution)
    assert Enum.count(all_available_steps) == 7
    available_step = Journey.Execution.get_next_available_step(execution)
    assert available_step[:name] == :first_name

    # Submit first_name.
    {:not_computed, _} = Journey.Execution.read_value(execution, :first_name)
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :first_name, "Luigi")
    {:computed, "Luigi"} = Journey.Execution.read_value(execution, :first_name)
    # Journey.Execution.get_summary(execution) |> IO.puts()

    # Submit birth day.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :birth_day, 21)
    {:computed, 21} = Journey.Execution.read_value(execution, :birth_day)
    all_values = Journey.Execution.get_all_values(execution.execution_id)
    assert Enum.count(all_values) == 7
    assert Journey.Execution.get_value(execution.execution_id, :birth_day) == 21

    # Get all values.
    values = Journey.Execution.get_all_values(execution)
    assert List.keymember?(values, :started_at, 0)
    assert List.keymember?(values, :first_name, 0)
    assert List.keymember?(values, :birth_month, 0)
    assert List.keymember?(values, :birth_day, 0)
    assert List.keymember?(values, :astrological_sign, 0)
    assert List.keymember?(values, :extra_something_for_taurus, 0)
    assert List.keymember?(values, :horoscope, 0)

    # Submit birth month.
    {:ok, execution} = Journey.Execution.update_value(execution.execution_id, :birth_month, :february)
    {:computed, "february"} = Journey.Execution.read_value(execution, :birth_month)
    # Journey.Execution.get_summary(execution) |> IO.puts()

    # Wait for the horoscope to be computed.
    case WaitForIt.wait(
           Journey.Execution.load!(execution.execution_id)
           |> Journey.Execution.get_unfilled_steps()
           |> Enum.count() == 0,
           timeout: 5_000,
           frequency: 1000
         ) do
      {:ok, _} ->
        true

      {:timeout, _timeout} ->
        {:ok, execution} = Journey.Execution.load(execution.execution_id)
        execution |> Journey.Execution.get_summary() |> IO.puts()
        assert false, "horoscope step never computed"
    end
  end

  test "basic pipeline test" do
    {:computed, result} =
      @process
      |> Journey.Process.execute()
      |> Journey.Execution.update_value!(:first_name, "Dory The Fish")
      |> Journey.Execution.update_value!(:birth_month, 3)
      |> Journey.Execution.update_value!(:birth_day, 18)
      |> Journey.Execution.wait_for_result(:horoscope)

    assert result == "You ain't got no... many things, but you got life."
  end
end
