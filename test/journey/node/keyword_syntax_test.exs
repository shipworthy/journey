defmodule Journey.Node.KeywordSyntaxTest do
  use ExUnit.Case, async: false

  import Journey.Node
  import Journey.Node.UpstreamDependencies
  import Journey.Helpers.Random, only: [random_string: 0]

  describe "keyword list syntax for gated_by" do
    test "single keyword condition is equivalent to unblocked_when" do
      # Using keyword list syntax
      graph1 =
        Journey.new_graph(
          "keyword_syntax_test #{random_string()}",
          "v1",
          [
            input(:value),
            compute(
              :alert,
              [value: fn node -> node.node_value > 40 end],
              fn _ -> {:ok, "alert!"} end
            )
          ]
        )

      # Using unblocked_when syntax
      graph2 =
        Journey.new_graph(
          "unblocked_when_test #{random_string()}",
          "v1",
          [
            input(:value),
            compute(
              :alert,
              unblocked_when(:value, fn node -> node.set_time != nil and node.node_value > 40 end),
              fn _ -> {:ok, "alert!"} end
            )
          ]
        )

      # Both should behave identically
      exec1 = Journey.start_execution(graph1)
      exec2 = Journey.start_execution(graph2)

      # Value below threshold - alert should not trigger
      exec1 = Journey.set(exec1, :value, 30)
      exec2 = Journey.set(exec2, :value, 30)

      assert Journey.get_value(exec1, :alert) == {:error, :not_set}
      assert Journey.get_value(exec2, :alert) == {:error, :not_set}

      # Value above threshold - alert should trigger
      exec1 = Journey.set(exec1, :value, 50)
      exec2 = Journey.set(exec2, :value, 50)

      assert {:ok, "alert!"} = Journey.get_value(exec1, :alert, wait_any: true)
      assert {:ok, "alert!"} = Journey.get_value(exec2, :alert, wait_any: true)
    end

    test "mixed list with atoms and keyword conditions" do
      graph =
        Journey.new_graph(
          "mixed_syntax_test #{random_string()}",
          "v1",
          [
            input(:x),
            input(:y),
            input(:threshold),
            compute(
              :result,
              [:x, :y, threshold: fn node -> node.node_value > 10 end],
              fn %{x: x, y: y, threshold: t} ->
                {:ok, "x=#{x}, y=#{y}, threshold=#{t}"}
              end
            )
          ]
        )

      exec = Journey.start_execution(graph)

      # Set x and y but threshold below limit
      exec = exec |> Journey.set(:x, 5) |> Journey.set(:y, 7) |> Journey.set(:threshold, 8)
      assert Journey.get_value(exec, :result) == {:error, :not_set}

      # Set threshold above limit - now should compute
      exec = Journey.set(exec, :threshold, 15)
      assert {:ok, "x=5, y=7, threshold=15"} = Journey.get_value(exec, :result, wait_any: true)
    end

    test "multiple keyword conditions create AND logic" do
      graph =
        Journey.new_graph(
          "multiple_keywords_test #{random_string()}",
          "v1",
          [
            input(:a),
            input(:b),
            compute(
              :result,
              [
                a: fn node -> node.node_value > 5 end,
                b: fn node -> node.node_value < 10 end
              ],
              fn %{a: a, b: b} -> {:ok, "a=#{a}, b=#{b}"} end
            )
          ]
        )

      exec = Journey.start_execution(graph)

      # Only a meets condition
      exec = exec |> Journey.set(:a, 7) |> Journey.set(:b, 15)
      assert Journey.get_value(exec, :result) == {:error, :not_set}

      # Only b meets condition
      exec = exec |> Journey.set(:a, 3) |> Journey.set(:b, 8)
      assert Journey.get_value(exec, :result) == {:error, :not_set}

      # Both meet conditions
      exec = exec |> Journey.set(:a, 7) |> Journey.set(:b, 8)
      assert {:ok, "a=7, b=8"} = Journey.get_value(exec, :result, wait_any: true)
    end

    test "keyword syntax works with mutate nodes" do
      graph =
        Journey.new_graph(
          "mutate_keyword_test #{random_string()}",
          "v1",
          [
            input(:value),
            input(:should_clear),
            mutate(
              :clear_value,
              [should_clear: fn node -> node.node_value == true end],
              fn _ -> {:ok, nil} end,
              mutates: :value
            )
          ]
        )

      exec = Journey.start_execution(graph)
      exec = Journey.set(exec, :value, "sensitive data")

      # Set should_clear to false - mutation should not happen
      exec = Journey.set(exec, :should_clear, false)
      assert Journey.get_value(exec, :value) == {:ok, "sensitive data"}

      # Set should_clear to true - mutation should happen
      exec = Journey.set(exec, :should_clear, true)
      assert {:ok, _} = Journey.get_value(exec, :clear_value, wait_any: true)
      exec = Journey.load(exec)
      assert Journey.get_value(exec, :value) == {:ok, nil}
    end

    test "keyword syntax works with schedule_once nodes" do
      graph =
        Journey.new_graph(
          "schedule_keyword_test #{random_string()}",
          "v1",
          [
            input(:enabled),
            schedule_once(
              :scheduled_task,
              [enabled: fn node -> node.node_value == true end],
              fn _ ->
                # Schedule for 1 second in the future
                {:ok, System.system_time(:second) + 1}
              end
            ),
            compute(
              :task_result,
              [:scheduled_task],
              fn _ -> {:ok, "task completed"} end
            )
          ]
        )

      exec = Journey.start_execution(graph)

      # Set enabled to false - schedule should not activate
      exec = Journey.set(exec, :enabled, false)
      assert Journey.get_value(exec, :scheduled_task) == {:error, :not_set}

      # Set enabled to true - schedule should activate
      exec = Journey.set(exec, :enabled, true)

      # Start background sweeps for test
      background_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(exec.id)

      assert {:ok, _time} = Journey.get_value(exec, :scheduled_task, wait_any: true)
      assert {:ok, "task completed"} = Journey.get_value(exec, :task_result, wait_any: true)

      Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_task)
    end
  end

  describe "conditional clearing with keyword list syntax" do
    test "downstream node is cleared when keyword condition becomes false" do
      graph =
        Journey.new_graph(
          "keyword_clearing_test #{random_string()}",
          "v1",
          [
            input(:temperature),
            compute(
              :heat_warning,
              [temperature: fn node -> node.node_value > 30 end],
              fn %{temperature: temp} ->
                {:ok, "Heat warning: #{temp}°C"}
              end
            )
          ]
        )

      exec = Journey.start_execution(graph)

      # Set temperature above threshold
      exec = Journey.set(exec, :temperature, 35)
      assert {:ok, "Heat warning: 35°C"} = Journey.get_value(exec, :heat_warning, wait_any: true)

      # Lower temperature below threshold - heat_warning should be cleared
      exec = Journey.set(exec, :temperature, 25)
      assert Journey.get_value(exec, :heat_warning) == {:error, :not_set}

      # Verify with values_all that it's really cleared
      values = Journey.values_all(exec)
      assert values.heat_warning == :not_set

      # Raise temperature again - should recompute
      exec = Journey.set(exec, :temperature, 40)
      assert {:ok, "Heat warning: 40°C"} = Journey.get_value(exec, :heat_warning, wait_any: true)
    end

    test "mixed list clears when keyword condition becomes false" do
      graph =
        Journey.new_graph(
          "mixed_clearing_test #{random_string()}",
          "v1",
          [
            input(:enabled),
            input(:value),
            compute(
              :alert,
              [:enabled, value: fn node -> node.node_value > 100 end],
              fn %{enabled: _, value: v} ->
                {:ok, "Alert: value is #{v}"}
              end
            )
          ]
        )

      exec = Journey.start_execution(graph)

      # Set all values with value above threshold
      exec =
        exec
        |> Journey.set(:enabled, true)
        |> Journey.set(:value, 150)

      assert {:ok, "Alert: value is 150"} = Journey.get_value(exec, :alert, wait_any: true)

      # Lower value below threshold - alert should be cleared even though :enabled is still set
      exec = Journey.set(exec, :value, 50)
      assert Journey.get_value(exec, :alert) == {:error, :not_set}

      # Verify enabled is still set but alert is cleared
      assert Journey.get_value(exec, :enabled) == {:ok, true}
      values = Journey.values_all(exec)
      assert values.alert == :not_set

      # Raise value again - should recompute
      exec = Journey.set(exec, :value, 200)
      assert {:ok, "Alert: value is 200"} = Journey.get_value(exec, :alert, wait_any: true)
    end

    test "keyword syntax and unblocked_when have identical clearing behavior" do
      # Graph with keyword syntax
      graph1 =
        Journey.new_graph(
          "keyword_graph #{random_string()}",
          "v1",
          [
            input(:x),
            input(:y),
            compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end),
            compute(
              :large_sum_alert,
              [sum: fn node -> node.node_value > 40 end],
              fn %{sum: s} -> {:ok, "Large sum: #{s}"} end
            )
          ]
        )

      # Graph with unblocked_when syntax
      graph2 =
        Journey.new_graph(
          "unblocked_when_graph #{random_string()}",
          "v1",
          [
            input(:x),
            input(:y),
            compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end),
            compute(
              :large_sum_alert,
              unblocked_when(:sum, fn node -> node.set_time != nil and node.node_value > 40 end),
              fn %{sum: s} -> {:ok, "Large sum: #{s}"} end
            )
          ]
        )

      exec1 = Journey.start_execution(graph1)
      exec2 = Journey.start_execution(graph2)

      # Set values that create large sum
      exec1 = exec1 |> Journey.set(:x, 30) |> Journey.set(:y, 30)
      exec2 = exec2 |> Journey.set(:x, 30) |> Journey.set(:y, 30)

      # Both should compute alert
      assert {:ok, 60} = Journey.get_value(exec1, :sum, wait_any: true)
      assert {:ok, 60} = Journey.get_value(exec2, :sum, wait_any: true)
      assert {:ok, "Large sum: 60"} = Journey.get_value(exec1, :large_sum_alert, wait_any: true)
      assert {:ok, "Large sum: 60"} = Journey.get_value(exec2, :large_sum_alert, wait_any: true)

      # Lower values to create small sum
      exec1 = exec1 |> Journey.set(:x, 10) |> Journey.set(:y, 10)
      exec2 = exec2 |> Journey.set(:x, 10) |> Journey.set(:y, 10)

      # Wait for sum recomputation
      assert {:ok, 20} = Journey.get_value(exec1, :sum, wait_new: true)
      assert {:ok, 20} = Journey.get_value(exec2, :sum, wait_new: true)

      # Both should have cleared the alert
      assert Journey.get_value(exec1, :large_sum_alert) == {:error, :not_set}
      assert Journey.get_value(exec2, :large_sum_alert) == {:error, :not_set}

      # Verify with values_all
      values1 = Journey.values_all(exec1)
      values2 = Journey.values_all(exec2)
      assert values1.large_sum_alert == :not_set
      assert values2.large_sum_alert == :not_set
    end

    test "multiple keyword conditions - clearing when any becomes false" do
      graph =
        Journey.new_graph(
          "multiple_conditions_clearing #{random_string()}",
          "v1",
          [
            input(:temp),
            input(:humidity),
            compute(
              :extreme_weather_alert,
              [
                temp: fn node -> node.node_value > 35 end,
                humidity: fn node -> node.node_value > 80 end
              ],
              fn %{temp: t, humidity: h} ->
                {:ok, "Extreme weather: #{t}°C, #{h}% humidity"}
              end
            )
          ]
        )

      exec = Journey.start_execution(graph)

      # Set both above thresholds
      exec = exec |> Journey.set(:temp, 40) |> Journey.set(:humidity, 90)

      assert {:ok, "Extreme weather: 40°C, 90% humidity"} =
               Journey.get_value(exec, :extreme_weather_alert, wait_any: true)

      # Lower temperature below threshold - alert should clear
      exec = Journey.set(exec, :temp, 30)
      assert Journey.get_value(exec, :extreme_weather_alert) == {:error, :not_set}

      # Lower humidity while temp is still low - alert should remain cleared
      exec = Journey.set(exec, :humidity, 70)
      assert Journey.get_value(exec, :extreme_weather_alert) == {:error, :not_set}

      # Raise temp back up, but humidity is now too low - alert should remain cleared
      exec = Journey.set(exec, :temp, 40)
      assert Journey.get_value(exec, :extreme_weather_alert) == {:error, :not_set}

      # Both above thresholds again - should recompute
      exec = Journey.set(exec, :humidity, 85)

      assert {:ok, "Extreme weather: 40°C, 85% humidity"} =
               Journey.get_value(exec, :extreme_weather_alert, wait_any: true)
    end
  end
end
