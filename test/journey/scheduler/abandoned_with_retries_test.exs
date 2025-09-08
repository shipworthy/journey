defmodule Journey.Scheduler.AbandonedWithRetriesTest do
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  alias Journey.Scheduler.Background.Sweeps.Abandoned

  use ExUnit.Case,
    async: true,
    parameterize:
      for(
        timeout_type <- [:timeout, :failure_after_timeout],
        do: %{timeout_type: timeout_type}
      )

  import Journey.Node

  import Journey.Node.UpstreamDependencies

  @tag :capture_log
  test "abandoned with retries", %{timeout_type: timeout_type} do
    run_test(timeout_type)
  end

  defp run_test(graph_type) do
    execution =
      create_graph(graph_type)
      |> Journey.start_execution()
      |> Journey.set_value(:birth_day, 26)
      |> Journey.set_value(:birth_month, "April")

    assert Journey.values_all(execution) |> redact([:execution_id, :last_updated_at]) == %{
             astrological_sign: :not_set,
             birth_day: {:set, 26},
             birth_month: {:set, "April"},
             first_name: :not_set,
             execution_id: {:set, "..."},
             last_updated_at: {:set, 1_234_567_890}
           }

    Process.sleep(2_000)

    assert 1 == Abandoned.sweep(execution.id)
    assert 1 == count_computations(execution.id, :astrological_sign, :abandoned)
    assert 1 == count_computations(execution.id, :astrological_sign, :computing)
    Process.sleep(2_000)

    assert 1 == Abandoned.sweep(execution.id)
    assert 2 == count_computations(execution.id, :astrological_sign, :abandoned)
    assert 0 == count_computations(execution.id, :astrological_sign, :computing)
    Process.sleep(2_000)
    assert 0 == Abandoned.sweep(execution.id)
    Process.sleep(2_000)
    assert 0 == Abandoned.sweep(execution.id)
    Process.sleep(2_000)
    assert 2 == count_computations(execution.id, :astrological_sign, :abandoned)
    assert 0 == count_computations(execution.id, :astrological_sign, :computing)

    assert Journey.values_all(execution) |> redact([:execution_id, :last_updated_at]) == %{
             astrological_sign: :not_set,
             birth_day: {:set, 26},
             birth_month: {:set, "April"},
             first_name: :not_set,
             execution_id: {:set, "..."},
             last_updated_at: {:set, 1_234_567_890}
           }
  end

  defp count_computations(execution_id, node_atom, state_atom) do
    execution_id
    |> Journey.load()
    |> Map.get(:computations)
    |> Enum.count(fn c -> c.node_name == node_atom and c.state == state_atom end)
  end

  defp create_graph(behavior) when behavior in [:success, :failure, :timeout, :failure_after_timeout] do
    Journey.new_graph(
      "astrological sign workflow, #{behavior} compute #{__MODULE__}",
      "v2.0.0",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(
          :astrological_sign,
          unblocked_when({:and, [{:birth_month, &provided?/1}, {:birth_day, &provided?/1}]}),
          fn %{birth_month: _birth_month, birth_day: _birth_day} ->
            case behavior do
              :timeout ->
                Process.sleep(:timer.seconds(5))
                {:ok, "Taurus"}

              :success ->
                {:ok, "Taurus"}

              :failure ->
                {:error, "simulated failure"}

              :failure_after_timeout ->
                Process.sleep(:timer.seconds(5))
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
