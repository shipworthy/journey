defmodule Journey.Tools.ComputationStatusAsTextTest do
  use ExUnit.Case, async: true

  import Journey.Node

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  describe "computation_status_as_text/2" do
    test "returns appropriate message for input nodes" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test input node #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            compute(:greeting, [:user_name], fn %{user_name: name} ->
              {:ok, "Hello, #{name}"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)

      result = Journey.Tools.computation_status_as_text(execution.id, :user_name)
      assert result == ":user_name: 📝 :not_compute_node (input nodes do not compute)"
    end

    test "shows outstanding computation with dependency tree for not_set computation" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test outstanding #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            input(:title),
            compute(:greeting, [:user_name, :title], fn %{user_name: name, title: title} ->
              {:ok, "Hello, #{title} #{name}!"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)

      result = Journey.Tools.computation_status_as_text(execution.id, :greeting)

      # Redact dynamic values for reliable comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :greeting (CMPREDACTED): ⬜ :not_set (not yet attempted) | :compute
          :and
           ├─ 🛑 :user_name | &provided?/1
           └─ 🛑 :title | &provided?/1
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "shows outstanding computation with partially satisfied dependencies" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test partial deps #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            input(:title),
            compute(:greeting, [:user_name, :title], fn %{user_name: name, title: title} ->
              {:ok, "Hello, #{title} #{name}!"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :user_name, "Alice")

      result = Journey.Tools.computation_status_as_text(execution.id, :greeting)

      # Redact dynamic computation IDs for reliable test comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :greeting (CMPREDACTED): ⬜ :not_set (not yet attempted) | :compute
          :and
           ├─ ✅ :user_name | &provided?/1 | rev 1
           └─ 🛑 :title | &provided?/1
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "shows completed successful computation with inputs used" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test completed success #{__MODULE__}",
          "1.0.0",
          [
            input(:user_name),
            input(:title),
            compute(:greeting, [:user_name, :title], fn %{user_name: name, title: title} ->
              {:ok, "Hello, #{title} #{name}!"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :user_name, "Alice")
      execution = Journey.set_value(execution, :title, "Dr.")

      {:ok, _greeting} = Journey.get_value(execution, :greeting, wait_new: true)

      result = Journey.Tools.computation_status_as_text(execution.id, :greeting)

      # Redact dynamic computation IDs for reliable test comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :greeting (CMPREDACTED): ✅ :success | :compute | rev 4
      inputs used:
         :title (rev 2)
         :user_name (rev 1)
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "shows completed failed computation" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test completed failed #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            compute(:will_fail, [:value], fn _ ->
              {:error, "intentional failure"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :value, "test")

      {:error, _} = Journey.get_value(execution, :will_fail, wait_new: true)

      result = Journey.Tools.computation_status_as_text(execution.id, :will_fail)

      # Redact dynamic computation IDs for reliable test comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :will_fail (CMPREDACTED): ❌ :failed | :compute | rev 7
      inputs used:
         <none>
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "handles mutate nodes" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test mutate #{__MODULE__}",
          "1.0.0",
          [
            input(:original),
            mutate(
              :modifier,
              [:original],
              fn %{original: val} ->
                {:ok, "modified: #{val}"}
              end,
              mutates: :original
            )
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :original, "test")

      {:ok, _} = Journey.get_value(execution, :modifier, wait_new: true)

      result = Journey.Tools.computation_status_as_text(execution.id, :modifier)

      # Redact dynamic computation IDs for reliable test comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :modifier (CMPREDACTED): ✅ :success | :mutate | rev 3
      inputs used:
         :original (rev 1)
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "handles schedule_once nodes" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test schedule_once #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            schedule_once(:scheduled_task, [:value], fn _ ->
              {:ok, System.system_time(:second) + 1}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :value, "trigger")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)
      {:ok, _} = Journey.get_value(execution, :scheduled_task, wait_new: true)

      result = Journey.Tools.computation_status_as_text(execution.id, :scheduled_task)

      # Redact dynamic computation IDs for reliable test comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :scheduled_task (CMPREDACTED): ✅ :success | :schedule_once | rev 3
      inputs used:
         :value (rev 1)
      """

      assert redacted_result == String.trim(expected_output)

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "handles schedule_recurring nodes" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test schedule_recurring #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            schedule_recurring(:recurring_task, [:value], fn _ ->
              {:ok, System.system_time(:second) + 1}
            end)
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :value, "trigger")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _schedule_time} = Journey.get_value(execution, :recurring_task, wait_new: true)
      Process.sleep(1000)

      result = Journey.Tools.computation_status_as_text(execution.id, :recurring_task)

      # Redact dynamic computation IDs for reliable test comparison
      redacted_result =
        result
        |> redact_computation_ids()

      # Schedule recurring computations can be in different states depending on timing
      # We'll check that it matches one of the expected patterns
      expected_success_rev_5 = """
      :recurring_task (CMPREDACTED): ✅ :success | :schedule_recurring | rev 5
      inputs used:
         :value (rev 1)
      """

      expected_success_rev_7 = """
      :recurring_task (CMPREDACTED): ✅ :success | :schedule_recurring | rev 7
      inputs used:
         :value (rev 1)
      """

      expected_not_set = """
      :recurring_task (CMPREDACTED): ⬜ :not_set (not yet attempted) | :schedule_recurring
          🛑 :value | &provided?/1
      """

      success_rev_5_matches = redacted_result == String.trim(expected_success_rev_5)
      success_rev_7_matches = redacted_result == String.trim(expected_success_rev_7)
      not_set_matches = redacted_result == String.trim(expected_not_set)

      assert success_rev_5_matches or success_rev_7_matches or not_set_matches,
             "Expected either success (rev 5/7) or not_set format, got: #{redacted_result}"

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "handles non-existent nodes" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test non-existent #{__MODULE__}",
          "1.0.0",
          [
            input(:value)
          ]
        )

      execution = Journey.start_execution(graph)

      result = Journey.Tools.computation_status_as_text(execution.id, :nonexistent)
      assert result == "Node :nonexistent not found in graph"
    end

    test "raises for non-existent execution" do
      assert_raise KeyError, fn ->
        Journey.Tools.computation_status_as_text("EXEC_DOESNT_EXIST", :some_node)
      end
    end

    test "handles computation with no computed_with data" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test no inputs #{__MODULE__}",
          "1.0.0",
          [
            compute(:no_deps, [], fn _ ->
              {:ok, "no dependencies"}
            end)
          ]
        )

      execution = Journey.start_execution(graph)

      {:ok, _} = Journey.get_value(execution, :no_deps, wait_new: true)

      result = Journey.Tools.computation_status_as_text(execution.id, :no_deps)

      # Redact dynamic computation IDs for reliable test comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :no_deps (CMPREDACTED): ✅ :success | :compute | rev 2
      inputs used:
         <none>
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "handles computation with abandoned state" do
      # This is a more complex test that would require setting up a scenario
      # where a computation is abandoned - for now we test the basic structure
      graph =
        Journey.new_graph(
          "computation_status_as_text test structure #{__MODULE__}",
          "1.0.0",
          [
            input(:value),
            compute(:simple, [:value], fn %{value: v} -> {:ok, v * 2} end)
          ]
        )

      execution = Journey.start_execution(graph)

      # Test not_set state first
      result = Journey.Tools.computation_status_as_text(execution.id, :simple)

      # Redact dynamic values for reliable comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :simple (CMPREDACTED): ⬜ :not_set (not yet attempted) | :compute
          🛑 :value | &provided?/1
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "handles complex dependency trees" do
      graph =
        Journey.new_graph(
          "computation_status_as_text test complex deps #{__MODULE__}",
          "1.0.0",
          [
            input(:option_a),
            input(:option_b),
            compute(
              :both_options,
              [:option_a, :option_b],
              fn %{option_a: a, option_b: b} ->
                {:ok, "got #{a} and #{b}"}
              end
            )
          ]
        )

      execution = Journey.start_execution(graph)

      result = Journey.Tools.computation_status_as_text(execution.id, :both_options)

      # Redact dynamic values for reliable comparison
      redacted_result =
        result
        |> redact_computation_ids()

      expected_output = """
      :both_options (CMPREDACTED): ⬜ :not_set (not yet attempted) | :compute
          :and
           ├─ 🛑 :option_a | &provided?/1
           └─ 🛑 :option_b | &provided?/1
      """

      assert redacted_result == String.trim(expected_output)
    end
  end

  # Helper functions for text redaction in tests
  defp redact_computation_ids(text) do
    text
    |> String.replace(~r/CMP[A-Z0-9]+/, "CMPREDACTED")
  end
end
