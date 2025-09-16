defmodule Journey.Scheduler.ConditionalClearingTest do
  use ExUnit.Case, async: true

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "conditional clearing when upstream values change" do
    test "downstream node should be cleared when its condition is no longer met" do
      f_add = fn %{x: x, y: y} -> {:ok, x + y} end

      graph =
        Journey.new_graph(
          "conditional clearing test #{Journey.Helpers.Random.random_string()}",
          "v1",
          [
            input(:x),
            input(:y),
            compute(:sum, [:x, :y], f_add),
            compute(
              :large_value_alert,
              unblocked_when(
                :sum,
                fn sum_node -> sum_node.set_time != nil and sum_node.node_value > 40 end
              ),
              fn _ -> {:ok, "🚨"} end
            )
          ]
        )

      execution = Journey.start_execution(graph)

      # Set initial values where sum is small (12 + 9 = 21)
      execution = Journey.set(execution, :x, 12)
      execution = Journey.set(execution, :y, 9)

      # Verify sum is computed but alert is not triggered
      assert {:ok, 21} = Journey.get_value(execution, :sum, wait_any: true)
      assert {:error, :not_set} = Journey.get_value(execution, :large_value_alert)

      # Increase y so sum exceeds threshold (12 + 100 = 112)
      execution = Journey.set(execution, :y, 100)

      # Wait for sum to be recomputed and verify both sum and alert are set
      assert {:ok, 112} = Journey.get_value(execution, :sum, wait_new: true)
      assert {:ok, "🚨"} = Journey.get_value(execution, :large_value_alert, wait_any: true)

      # Decrease y so sum is below threshold again (12 + 1 = 13)
      execution = Journey.set(execution, :y, 1)

      # Wait for sum to be recomputed
      assert {:ok, 13} = Journey.get_value(execution, :sum, wait_new: true)

      # CRITICAL: Alert should be cleared since condition is no longer met
      assert {:error, :not_set} = Journey.get_value(execution, :large_value_alert),
             "Alert should be cleared when sum drops below threshold"

      # Double-check with values_all to ensure it's really not set
      values = Journey.values_all(execution)

      assert values.large_value_alert == :not_set,
             "large_value_alert should be :not_set but was #{inspect(values.large_value_alert)}"
    end

    test "multiple conditional nodes should be managed correctly" do
      graph =
        Journey.new_graph(
          "multiple conditions test #{Journey.Helpers.Random.random_string()}",
          "v1",
          [
            input(:weather),
            compute(
              :bring_umbrella,
              unblocked_when(:weather, &true?/1),
              fn _ -> {:ok, "☂️"} end
            ),
            compute(
              :bring_sunglasses,
              unblocked_when(:weather, &false?/1),
              fn _ -> {:ok, "🕶️"} end
            )
          ]
        )

      execution = Journey.start_execution(graph)

      # Set weather to rainy (true)
      execution = Journey.set(execution, :weather, true)
      assert {:ok, "☂️"} = Journey.get_value(execution, :bring_umbrella, wait_any: true)
      assert {:error, :not_set} = Journey.get_value(execution, :bring_sunglasses)

      # Change weather to sunny (false)
      execution = Journey.set(execution, :weather, false)
      assert {:ok, "🕶️"} = Journey.get_value(execution, :bring_sunglasses, wait_any: true)

      # CRITICAL: Umbrella should now be cleared
      assert {:error, :not_set} = Journey.get_value(execution, :bring_umbrella),
             "bring_umbrella should be cleared when weather is no longer rainy"

      # Verify with values_all
      values = Journey.values_all(execution)

      assert values.bring_umbrella == :not_set,
             "bring_umbrella should be :not_set but was #{inspect(values.bring_umbrella)}"

      assert values.bring_sunglasses == {:set, "🕶️"}
    end
  end
end
