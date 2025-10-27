defmodule Journey.Scheduler.MutateUpdateRevisionTest do
  use ExUnit.Case, async: true

  require Logger
  import Journey.Node

  describe "mutate with update_revision_on_change |" do
    test "mutate with update_revision_on_change: true triggers downstream recomputation" do
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
              update_revision_on_change: true
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

      # Temperature alert should RECOMPUTE because update_revision_on_change: true
      {:ok, "High temperature: 40°C", _} = Journey.get(execution, :temperature_alert, wait: :newer)
    end

    test "mutate with update_revision_on_change: false (default) does NOT trigger downstream recomputation" do
      graph =
        Journey.new_graph(
          "mutate update_revision false test",
          "v1.0.0",
          [
            input(:trigger),
            input(:counter),
            mutate(
              :increment_counter,
              [:trigger],
              fn %{counter: c} ->
                {:ok, c + 1}
              end,
              mutates: :counter
            ),
            compute(
              :new_counter_notification,
              [:counter],
              fn %{counter: c} ->
                {:ok, "new counter: #{c}"}
              end
            )
          ]
        )

      # Set an initial value for the counter.
      execution =
        graph
        |> Journey.start_execution()
        |> Journey.set(:counter, 1)

      # Take a note of the counter's revision.
      {:ok, 1, rev_counter_initial} = Journey.get(execution, :counter)

      # Make sure the counter value triggers a downstream computation.
      {:ok, ncn, rev_new_counter_notification_initial} = Journey.get(execution, :new_counter_notification, wait: :any)
      assert ncn == "new counter: 1"

      # Trigger an increment mutation for the counter.
      execution = execution |> Journey.set(:trigger, "mutation trigger 1")

      # Make sure mutation takes place, and updates the value of the counter.
      {:ok, mutation_value, _mutation_revision} = Journey.get(execution, :increment_counter, wait: :any)
      assert mutation_value == "updated :counter"

      # Make sure the increment happened (we already waited for the mutation to complete, so no need to wait any more).
      {:ok, new_counter_value, rev_new_counter} = Journey.get(execution, :counter, wait: :any)
      assert new_counter_value == 2

      # By default, mutations don't increment the revision of the mutated node.
      assert rev_counter_initial == rev_new_counter

      # The mutated value of counter will not trigger a downstream computation -- no new values, no new revisions.
      {:error, :not_set} =
        Journey.get(execution, :new_counter_notification,
          wait: {:newer_than, rev_new_counter_notification_initial},
          timeout: 2000
        )

      {:ok, ncn2, rev_ncn2} = Journey.get(execution, :new_counter_notification, wait: :any)
      assert ncn2 == "new counter: 1"
      assert rev_ncn2 == rev_new_counter_notification_initial
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
              update_revision_on_change: true
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
      {:ok, eta1, eta1_rev} = Journey.get(execution, :arrival_eta, wait: :newer)
      # After mutation: location = 10, ETA = (100-10)/10 = 9 minutes
      assert eta1 == "ETA: 9 minutes"

      # Wait for scheduled update to trigger mutation (must be newer than rev1)
      {:ok, "updated :driver_location", rev2} =
        Journey.get(execution, :update_location_from_gps, wait: {:newer_than, rev1})

      assert rev2 > rev1

      # ETA should recompute with new location (20), ETA = (100-20)/10 = 8 minutes
      # Wait for ETA newer than eta1_rev to ensure we get the NEXT computation, not a later one
      {:ok, eta2, eta2_rev} = Journey.get(execution, :arrival_eta, wait: {:newer_than, eta1_rev})
      assert eta2 == "ETA: 8 minutes"
      assert eta2_rev > eta1_rev

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
              update_revision_on_change: true
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

    test "mutate with update_revision_on_change: true and unchanged value does NOT trigger downstream recomputation" do
      graph =
        Journey.new_graph(
          "mutate unchanged value test",
          "v1.0.0",
          [
            input(:trigger),
            input(:status),
            mutate(
              :refresh_status,
              [:trigger],
              fn %{status: current_status} ->
                # Simulate polling that returns the same value
                {:ok, current_status}
              end,
              mutates: :status,
              update_revision_on_change: true
            ),
            compute(
              :status_display,
              [:status],
              fn %{status: s} ->
                {:ok, "Status: #{s}"}
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set initial status
      execution = Journey.set(execution, :status, "active")
      {:ok, initial_revision} = Journey.get(execution, :status) |> elem(2) |> then(&{:ok, &1})

      # Trigger mutation
      execution = Journey.set(execution, :trigger, 1)

      # Wait for mutation to complete
      {:ok, "updated :status", _} = Journey.get(execution, :refresh_status, wait: :any)

      # Status display should compute initially
      {:ok, "Status: active", initial_display_rev} = Journey.get(execution, :status_display, wait: :any)

      # Get the status revision after mutation
      {:ok, "active", status_rev_after_mutation} = Journey.get(execution, :status)

      # Revision should be UNCHANGED because value didn't change (matching Journey.set/3 behavior)
      assert status_rev_after_mutation == initial_revision

      # Trigger another mutation with same value
      execution = Journey.set(execution, :trigger, 2)

      # Wait for mutation to complete
      {:ok, "updated :status", _} = Journey.get(execution, :refresh_status, wait: :newer)

      # Status display should NOT recompute because status revision didn't change
      {:error, :not_set} =
        Journey.get(execution, :status_display,
          wait: {:newer_than, initial_display_rev},
          timeout: 2000
        )

      # Verify status display is still the initial computation
      {:ok, "Status: active", final_display_rev} = Journey.get(execution, :status_display)
      assert final_display_rev == initial_display_rev
    end

    test "mutate with update_revision_on_change: true and changed value DOES trigger downstream recomputation" do
      graph =
        Journey.new_graph(
          "mutate changed value test",
          "v1.0.0",
          [
            input(:trigger),
            input(:counter),
            mutate(
              :increment,
              [:trigger],
              fn %{counter: c} ->
                # Value changes each time
                {:ok, c + 1}
              end,
              mutates: :counter,
              update_revision_on_change: true
            ),
            compute(
              :counter_display,
              [:counter],
              fn %{counter: c} ->
                {:ok, "Count: #{c}"}
              end
            )
          ]
        )

      execution = graph |> Journey.start_execution()

      # Set initial counter
      execution = Journey.set(execution, :counter, 0)
      {:ok, initial_revision} = Journey.get(execution, :counter) |> elem(2) |> then(&{:ok, &1})

      # Trigger mutation
      execution = Journey.set(execution, :trigger, 1)

      # Wait for mutation to complete
      {:ok, "updated :counter", _} = Journey.get(execution, :increment, wait: :any)

      # Counter should have new value AND new revision
      {:ok, 1, rev_after_first_mutation} = Journey.get(execution, :counter)
      assert rev_after_first_mutation > initial_revision

      # Counter display should recompute
      {:ok, "Count: 1", _} = Journey.get(execution, :counter_display, wait: :any)

      # Trigger another mutation with different value
      execution = Journey.set(execution, :trigger, 2)

      # Wait for mutation to complete
      {:ok, "updated :counter", _} = Journey.get(execution, :increment, wait: :newer)

      # Counter should have new value AND new revision
      {:ok, 2, rev_after_second_mutation} = Journey.get(execution, :counter)
      assert rev_after_second_mutation > rev_after_first_mutation

      # Counter display should recompute with new value
      {:ok, "Count: 2", _} = Journey.get(execution, :counter_display, wait: :newer)
    end
  end
end
