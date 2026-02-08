defmodule Journey.Scheduler.ComputeIdempotencyTest do
  use ExUnit.Case, async: true

  require Logger
  import Journey.Node

  describe "compute node idempotency |" do
    test "compute node with unchanged value does NOT trigger downstream recomputation" do
      graph =
        Journey.new_graph(
          "compute idempotency test - unchanged value",
          "v1.0.0",
          [
            input(:temperature),
            # This compute node will return the same result when temperature changes within same range
            compute(
              :temperature_status,
              [:temperature],
              fn %{temperature: temp} ->
                # Returns "normal" for any temp < 30
                if temp < 30 do
                  {:ok, "normal"}
                else
                  {:ok, "high"}
                end
              end
            ),
            # Downstream node that should only recompute when temperature_status changes
            compute(
              :alert_message,
              [:temperature_status],
              fn %{temperature_status: status} ->
                {:ok, "Alert: #{status}"}
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set initial temperature
      execution = Journey.set(execution, :temperature, 20)

      # Wait for initial computation
      {:ok, "normal", initial_status_rev} = Journey.get(execution, :temperature_status, wait: :any)

      # Wait for downstream to compute
      {:ok, "Alert: normal", initial_alert_rev} = Journey.get(execution, :alert_message, wait: :any)

      # Change temperature but keep it in same range (still < 30)
      # temperature_status will return the same value ("normal")
      execution = Journey.set(execution, :temperature, 25)

      # Wait for temperature_status to recompute
      # The computation executes, but since value is unchanged, revision should not change
      :timer.sleep(1000)

      # Get current status revision - should be UNCHANGED because value didn't change
      {:ok, "normal", status_rev_after_change} = Journey.get(execution, :temperature_status)

      assert status_rev_after_change == initial_status_rev,
             "temperature_status revision should not change when value is unchanged (matching Journey.set/3 behavior)"

      # Downstream alert_message should NOT recompute because temperature_status revision didn't change
      {:error, :not_set} =
        Journey.get(execution, :alert_message,
          wait: {:newer_than, initial_alert_rev},
          timeout: 2000
        )

      # Verify alert_message is still the initial computation
      {:ok, "Alert: normal", final_alert_rev} = Journey.get(execution, :alert_message)
      assert final_alert_rev == initial_alert_rev
    end

    test "compute node with changed value DOES trigger downstream recomputation" do
      graph =
        Journey.new_graph(
          "compute idempotency test - changed value",
          "v1.0.0",
          [
            input(:temperature),
            # This compute node will return different results based on temperature
            compute(
              :temperature_status,
              [:temperature],
              fn %{temperature: temp} ->
                if temp < 30 do
                  {:ok, "normal"}
                else
                  {:ok, "high"}
                end
              end
            ),
            # Downstream node
            compute(
              :alert_message,
              [:temperature_status],
              fn %{temperature_status: status} ->
                {:ok, "Alert: #{status}"}
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set initial temperature to get "normal" status
      execution = Journey.set(execution, :temperature, 20)

      # Wait for initial computation
      {:ok, "normal", initial_status_rev} = Journey.get(execution, :temperature_status, wait: :any)

      # Wait for downstream to compute
      {:ok, "Alert: normal", initial_alert_rev} = Journey.get(execution, :alert_message, wait: :any)

      # Change temperature to trigger different status ("high")
      execution = Journey.set(execution, :temperature, 40)

      # temperature_status should recompute with new value and NEW revision
      {:ok, "high", status_rev_after_change} =
        Journey.get(execution, :temperature_status, wait: {:newer_than, initial_status_rev})

      assert status_rev_after_change > initial_status_rev,
             "temperature_status revision should increase when value changes"

      # Downstream alert_message SHOULD recompute because temperature_status changed
      {:ok, "Alert: high", new_alert_rev} =
        Journey.get(execution, :alert_message, wait: {:newer_than, initial_alert_rev})

      assert new_alert_rev > initial_alert_rev
    end

    test "multiple triggers with alternating same/different values" do
      graph =
        Journey.new_graph(
          "compute idempotency test - mixed changes",
          "v1.0.0",
          [
            input(:value),
            input(:recompute_counter),
            # Compute node that rounds to nearest 10
            compute(
              :rounded_value,
              [:value],
              fn %{value: v} ->
                rounded = div(v + 5, 10) * 10
                {:ok, rounded}
              end
            ),
            # Downstream mutate that increments a counter
            mutate(
              :increment_counter,
              [:rounded_value],
              fn %{recompute_counter: count} ->
                {:ok, (count || 0) + 1}
              end,
              mutates: :recompute_counter,
              update_revision_on_change: true
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set initial counter
      execution = Journey.set(execution, :recompute_counter, 0)

      # Set initial value: 12 -> rounds to 10
      execution = Journey.set(execution, :value, 12)
      {:ok, 10, rounded_rev1} = Journey.get(execution, :rounded_value, wait: :any)
      {:ok, "updated :recompute_counter", inc_rev1} = Journey.get(execution, :increment_counter, wait: :any)
      {:ok, 1, _} = Journey.get(execution, :recompute_counter)

      # Change to 14 -> still rounds to 10 (unchanged)
      execution = Journey.set(execution, :value, 14)
      :timer.sleep(1000)
      {:ok, count_after_14, _} = Journey.get(execution, :recompute_counter)
      assert count_after_14 == 1, "recompute_counter should not increase when rounded_value unchanged"

      # Change to 18 -> rounds to 20 (changed!)
      execution = Journey.set(execution, :value, 18)
      {:ok, 20, rounded_rev2} = Journey.get(execution, :rounded_value, wait: {:newer_than, rounded_rev1})

      {:ok, "updated :recompute_counter", inc_rev2} =
        Journey.get(execution, :increment_counter, wait: {:newer_than, inc_rev1})

      {:ok, 2, _} = Journey.get(execution, :recompute_counter)

      # Change to 22 -> still rounds to 20 (unchanged)
      execution = Journey.set(execution, :value, 22)
      :timer.sleep(1000)
      {:ok, count_after_22, _} = Journey.get(execution, :recompute_counter)
      assert count_after_22 == 2, "recompute_counter should not increase when rounded_value unchanged"

      # Change to 35 -> rounds to 40 (changed!)
      execution = Journey.set(execution, :value, 35)
      {:ok, 40, _} = Journey.get(execution, :rounded_value, wait: {:newer_than, rounded_rev2})

      {:ok, "updated :recompute_counter", _} =
        Journey.get(execution, :increment_counter, wait: {:newer_than, inc_rev2})

      {:ok, 3, _} = Journey.get(execution, :recompute_counter)
    end
  end
end
