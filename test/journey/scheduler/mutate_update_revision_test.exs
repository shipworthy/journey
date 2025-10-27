defmodule Journey.Scheduler.MutateUpdateRevisionTest do
  use ExUnit.Case, async: true

  require Logger
  import Journey.Node

  describe "mutate with update_revision |" do
    test "mutate with update_revision: true triggers downstream recomputation" do
      graph =
        Journey.new_graph(
          "mutate update_revision true test",
          "v1.0.0",
          [
            input(:trigger),
            input(:temperature),
            mutate(
              :update_temperature,
              [:trigger],
              fn %{temperature: temp} ->
                # Update temperature from external source
                {:ok, (temp || 20) + 10}
              end,
              mutates: :temperature,
              update_revision: true
            ),
            compute(
              :temperature_alert,
              [:temperature],
              fn %{temperature: temp} ->
                if temp > 30 do
                  {:ok, "High temperature: #{temp}°C"}
                else
                  {:ok, "Normal temperature: #{temp}°C"}
                end
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set initial temperature and trigger
      execution = Journey.set(execution, :temperature, 20)
      execution = Journey.set(execution, :trigger, 1)

      # Wait for mutation to complete
      {:ok, "updated :temperature", _} = Journey.get(execution, :update_temperature, wait: :any)

      # Temperature alert should compute with the mutated value (30)
      {:ok, "Normal temperature: 30°C", _} = Journey.get(execution, :temperature_alert, wait: :any)

      # Trigger another update
      execution = Journey.set(execution, :trigger, 2)

      # Wait for mutation to complete
      {:ok, "updated :temperature", _} = Journey.get(execution, :update_temperature, wait: :newer)

      # Temperature alert should RECOMPUTE because update_revision: true
      {:ok, "High temperature: 40°C", _} = Journey.get(execution, :temperature_alert, wait: :newer)
    end

    test "mutate with update_revision: false (default) does NOT trigger downstream recomputation" do
      graph =
        Journey.new_graph(
          "mutate update_revision false test",
          "v1.0.0",
          [
            input(:trigger),
            input(:value),
            mutate(
              :change_value,
              [:trigger],
              fn _ ->
                {:ok, "mutated"}
              end,
              mutates: :value,
              update_revision: false
            ),
            compute(
              :downstream,
              [:value],
              fn %{value: v} ->
                {:ok, "computed with: #{v}"}
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set trigger first to cause mutation
      execution = Journey.set(execution, :trigger, 1)
      execution = Journey.set(execution, :value, "original")

      # Wait for mutation to complete - it will mutate value to "mutated"
      {:ok, "updated :value", _} = Journey.get(execution, :change_value, wait: :any)

      # Downstream computes with the mutated value
      {:ok, "computed with: mutated", rev1} = Journey.get(execution, :downstream, wait: :any)

      # Trigger another mutation
      execution = Journey.set(execution, :trigger, 2)

      # Wait for mutation to complete
      {:ok, "updated :value", _} = Journey.get(execution, :change_value, wait: :newer)

      # Downstream should NOT recompute - revision should be the same
      {:ok, "computed with: mutated", ^rev1} = Journey.get(execution, :downstream)
    end

    test "external polling pattern: schedule_recurring + mutate + compute downstream" do
      graph =
        Journey.new_graph(
          "external polling pattern test",
          "v1.0.0",
          [
            schedule_recurring(
              :location_poll_schedule,
              [],
              fn _ ->
                # Schedule every 2 seconds
                {:ok, System.system_time(:second) + 2}
              end
            ),
            input(:driver_location),
            mutate(
              :update_location_from_gps,
              [:location_poll_schedule],
              fn %{driver_location: current_location} ->
                # Simulate fetching from GPS - increment location
                new_location = (current_location || 0) + 10
                {:ok, new_location}
              end,
              mutates: :driver_location,
              update_revision: true
            ),
            compute(
              :arrival_eta,
              [:driver_location],
              fn %{driver_location: location} ->
                # Calculate ETA based on distance (destination at 100)
                distance = 100 - location
                eta_minutes = max(0, div(distance, 10))
                {:ok, "ETA: #{eta_minutes} minutes"}
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Start background sweeps for scheduled nodes
      background_sweeps_task =
        Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)

      # Set initial location
      execution = Journey.set(execution, :driver_location, 0)

      # Wait for schedule to fire and trigger first mutation
      {:ok, "updated :driver_location", rev1} =
        Journey.get(execution, :update_location_from_gps, wait: :any)

      # ETA should recompute after mutation (location becomes 10)
      {:ok, eta1, _} = Journey.get(execution, :arrival_eta, wait: :newer)
      # After mutation: location = 10, ETA = (100-10)/10 = 9 minutes
      assert eta1 == "ETA: 9 minutes"

      # Wait for scheduled update to trigger mutation (must be newer than rev1)
      {:ok, "updated :driver_location", rev2} =
        Journey.get(execution, :update_location_from_gps, wait: {:newer_than, rev1})

      assert rev2 > rev1

      # ETA should recompute with new location (20), ETA = (100-20)/10 = 8 minutes
      {:ok, eta2, _} = Journey.get(execution, :arrival_eta, wait: :newer)
      assert eta2 == "ETA: 8 minutes"

      Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "multiple downstream nodes all recompute when mutation with update_revision occurs" do
      graph =
        Journey.new_graph(
          "multiple downstream test",
          "v1.0.0",
          [
            input(:trigger),
            input(:price),
            mutate(
              :apply_discount,
              [:trigger],
              fn %{price: p} ->
                {:ok, p * 0.9}
              end,
              mutates: :price,
              update_revision: true
            ),
            compute(
              :price_with_tax,
              [:price],
              fn %{price: p} ->
                {:ok, p * 1.1}
              end
            ),
            compute(
              :price_display,
              [:price],
              fn %{price: p} ->
                {:ok, "$#{Float.round(p, 2)}"}
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set initial price and trigger
      execution = Journey.set(execution, :price, 100.0)
      execution = Journey.set(execution, :trigger, 1)

      # Wait for mutation
      {:ok, "updated :price", _} = Journey.get(execution, :apply_discount, wait: :any)

      # Wait for both downstream computations
      {:ok, price_with_tax1, _} = Journey.get(execution, :price_with_tax, wait: :any)
      {:ok, price_display1, _} = Journey.get(execution, :price_display, wait: :any)

      # Verify they computed with the discounted price (90)
      assert_in_delta price_with_tax1, 99.0, 0.01
      assert price_display1 == "$90.0"

      # Trigger another discount
      execution = Journey.set(execution, :trigger, 2)

      # Wait for mutation
      {:ok, "updated :price", _} = Journey.get(execution, :apply_discount, wait: :newer)

      # Both downstream should recompute
      {:ok, price_with_tax2, _} = Journey.get(execution, :price_with_tax, wait: :newer)
      {:ok, price_display2, _} = Journey.get(execution, :price_display, wait: :newer)

      # Verify they computed with the newly discounted price (90 * 0.9 = 81)
      assert_in_delta price_with_tax2, 89.1, 0.01
      assert price_display2 == "$81.0"
    end
  end
end
