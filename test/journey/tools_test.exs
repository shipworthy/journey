defmodule Journey.ToolsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Executions, only: [find_computations_by_node_name: 2]
  import WaitForIt

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  describe "summarize_as_data/1" do
    test "returns structured data for new execution" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      summary_data = Journey.Tools.summarize_as_data(execution.id)

      assert summary_data.execution_id == execution.id
      assert summary_data.graph_name == "test graph 1 Elixir.Journey.Test.Support"
      assert summary_data.graph_version == "1.0.0"
      assert summary_data.archived_at == nil
      assert is_integer(summary_data.created_at)
      assert is_integer(summary_data.updated_at)
      assert is_integer(summary_data.duration_seconds)
      assert is_integer(summary_data.revision)

      assert Map.has_key?(summary_data.values, :set)
      assert Map.has_key?(summary_data.values, :not_set)
      assert Map.has_key?(summary_data.computations, :completed)
      assert Map.has_key?(summary_data.computations, :outstanding)
      assert summary_data.graph != nil
    end

    test "no such execution" do
      graph = Journey.Test.Support.create_test_graph1()
      _execution = Journey.start_execution(graph)

      assert_raise ArgumentError, "Execution 'none such' not found", fn ->
        Journey.Tools.summarize_as_data("none such")
      end
    end

    test "returns structured data for progressed execution" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "John Doe")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _, _} = Journey.get(execution, :reminder, wait: :any)

      summary_data = Journey.Tools.summarize_as_data(execution.id)

      assert summary_data.execution_id == execution.id
      assert summary_data.graph_name == "test graph 1 Elixir.Journey.Test.Support"
      assert summary_data.graph_version == "1.0.0"
      assert summary_data.archived_at == nil

      # Should have some set values after execution progresses
      refute Enum.empty?(summary_data.values.set)

      # Should have some completed computations
      refute Enum.empty?(summary_data.computations.completed)

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end

  describe "introspect/1 (text formatting)" do
    test "formats complete execution summary text for new execution" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      result = Journey.Tools.introspect(execution.id)

      # Get actual system node values for expected output
      values = Journey.values(execution)
      last_updated_at_value = Map.get(values, :last_updated_at)
      execution_id_value = Map.get(values, :execution_id)

      # Redact dynamic values for reliable comparison
      redacted_result =
        result
        # |> redact_text_execution_id(execution.id)
        |> redact_text_timestamps()
        |> redact_text_duration()
        |> redact_text_seconds_ago()

      # Complete expected output as living documentation for engineers
      expected_output = """
      Execution summary:
      - ID: '#{execution.id}'
      - Graph: 'test graph 1 Elixir.Journey.Test.Support' | '1.0.0'
      - Archived at: not archived
      - Created at: REDACTED UTC | REDACTED seconds ago
      - Last updated at: REDACTED UTC | REDACTED seconds ago
      - Duration: REDACTED seconds
      - Revision: 0
      - # of Values: 2 (set) / 6 (total)
      - # of Computations: 3

      Values:
      - Set:
        - execution_id: '#{execution_id_value}' | :input
          set at REDACTED | rev: 0

        - last_updated_at: '#{last_updated_at_value}' | :input
          set at REDACTED | rev: 0


      - Not set:
        - greeting: <unk> | :compute
        - reminder: <unk> | :compute
        - time_to_issue_reminder_schedule: <unk> | :tick_once
        - user_name: <unk> | :input  

      Computations:
      - Completed:


      - Outstanding:
        - time_to_issue_reminder_schedule: ⬜ :not_set (not yet attempted) | :tick_once
             🛑 :greeting | &provided?/1
        - reminder: ⬜ :not_set (not yet attempted) | :compute
             🛑 :time_to_issue_reminder_schedule | &provided?/1
        - greeting: ⬜ :not_set (not yet attempted) | :compute
             🛑 :user_name | &provided?/1
      """

      assert redacted_result == String.trim(expected_output)
    end

    test "formats complete execution summary text for progressed execution" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set(:user_name, "John Doe")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _, _} = Journey.get(execution, :reminder, wait: :any)

      result = Journey.Tools.introspect(execution.id)

      # Get actual values for expected output
      values = Journey.values(execution)
      last_updated_at_value = Map.get(values, :last_updated_at)
      greeting_value = Map.get(values, :greeting)
      reminder_value = Map.get(values, :reminder)
      time_to_issue_reminder_schedule_value = Map.get(values, :time_to_issue_reminder_schedule)
      user_name_value = Map.get(values, :user_name)
      execution_id_value = Map.get(values, :execution_id)

      # Redact dynamic values for reliable comparison
      redacted_result =
        result
        |> redact_text_timestamps()
        |> redact_text_duration()
        |> redact_text_seconds_ago()
        |> String.replace(~r/CMP[A-Z0-9]+/, "CMPREDACTED")

      expected_output = """
      Execution summary:
      - ID: '#{execution.id}'
      - Graph: 'test graph 1 Elixir.Journey.Test.Support' | '1.0.0'
      - Archived at: not archived
      - Created at: REDACTED UTC | REDACTED seconds ago
      - Last updated at: REDACTED UTC | REDACTED seconds ago
      - Duration: REDACTED seconds
      - Revision: 7
      - # of Values: 6 (set) / 6 (total)
      - # of Computations: 3

      Values:
      - Set:
        - last_updated_at: '#{last_updated_at_value}' | :input
          set at REDACTED | rev: 7

        - reminder: '#{inspect(reminder_value)}' | :compute
          computed at REDACTED | rev: 7

        - time_to_issue_reminder_schedule: '#{time_to_issue_reminder_schedule_value}' | :tick_once
          computed at REDACTED | rev: 5

        - greeting: '#{inspect(greeting_value)}' | :compute
          computed at REDACTED | rev: 3

        - user_name: '#{inspect(user_name_value)}' | :input
          set at REDACTED | rev: 1

        - execution_id: '#{execution_id_value}' | :input
          set at REDACTED | rev: 0


      - Not set:
        

      Computations:
      - Completed:
        - :reminder (CMPREDACTED): ✅ :success | :compute | rev 7
          inputs used: 
             :time_to_issue_reminder_schedule (rev 5)
        - :time_to_issue_reminder_schedule (CMPREDACTED): ✅ :success | :tick_once | rev 5
          inputs used: 
             :greeting (rev 3)
        - :greeting (CMPREDACTED): ✅ :success | :compute | rev 3
          inputs used: 
             :user_name (rev 1)

      - Outstanding:
      """

      assert redacted_result == String.trim_leading(expected_output)

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "uses emoji format for computation states" do
      # Create a graph with multiple compute nodes
      graph =
        Journey.new_graph("emoji test graph #{random_string()}", "v1.0.0", [
          input(:value),
          compute(:success_node, [:value], fn %{value: v} -> {:ok, v * 2} end),
          compute(:fail_node, [:value], fn _deps -> {:error, "intentional failure"} end)
        ])

      execution = Journey.start_execution(graph)

      # Check initial state shows outstanding computations
      summary_initial_text = Journey.Tools.introspect(execution.id)
      assert summary_initial_text =~ "⬜ :not_set (not yet attempted)"
      assert summary_initial_text =~ "🛑 :value | &provided?/1"

      # Set value to trigger computations
      execution = Journey.set(execution, :value, 10)

      # Start background sweeps to process computations
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Get values to trigger computations
      {:ok, _, _} = Journey.get(execution, :success_node, wait: :any)
      # The fail_node will fail
      {:error, _error} = Journey.get(execution, :fail_node, wait: :newer)

      # Check that completed states use emoji format
      _execution_after = Journey.load(execution.id)

      # Format as text and check for emoji usage
      summary_text = Journey.Tools.introspect(execution.id)

      # Should see success emoji for successful computation
      assert summary_text =~ "✅ :success"

      # Should see failure emoji for failed computation
      assert summary_text =~ "❌ :failed"

      # Verify the computation state text helper is being used properly
      assert Journey.Tools.computation_state_to_text(:success) == "✅ :success"
      assert Journey.Tools.computation_state_to_text(:failed) == "❌ :failed"
      assert Journey.Tools.computation_state_to_text(:not_set) == "⬜ :not_set (not yet attempted)"

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "formats complex not conditions in mixed logical operators" do
      # Create a comprehensive graph with various :not condition combinations
      graph =
        Journey.new_graph("Complex Not Conditions Test #{random_string()}", "v1.0.0", [
          # Input nodes
          input(:user_applied),
          input(:user_approved),
          input(:user_requested_card),
          input(:card_mailed),
          input(:user_name),

          # 1. Simple condition (no :not)
          compute(:send_welcome, [:user_name], fn _ -> {:ok, "Welcome!"} end),

          # 2. Single :not condition
          compute(
            :send_reminder,
            {:not, {:user_requested_card, &true?/1}},
            fn _ -> {:ok, "Please request your card"} end
          ),

          # 3. Complex :and with mixed :not and direct conditions
          compute(
            :send_approval_notice,
            {
              :and,
              [
                {:user_applied, &true?/1},
                {:user_approved, &true?/1},
                {:not, {:user_requested_card, &true?/1}}
              ]
            },
            fn _ -> {:ok, "Congratulations! You're approved"} end
          ),

          # 4. Complex :or with multiple :not conditions
          compute(
            :send_follow_up,
            {
              :or,
              [
                {:not, {:user_applied, &true?/1}},
                {:not, {:card_mailed, &true?/1}}
              ]
            },
            fn _ -> {:ok, "Follow up message"} end
          ),
          # 4. Complex :or with multiple unmet conditions
          compute(
            :user_applied_or_card_mailed,
            {
              :or,
              [
                {:user_applied, &true?/1},
                {:card_mailed, &true?/1}
              ]
            },
            fn _ -> {:ok, "Follow up message"} end
          )
        ])

      execution = Journey.start_execution(graph)

      # Wait for the immediately unblocked computations to complete
      # This ensures deterministic test output
      {:ok, _, _} = Journey.get(execution, :send_reminder, wait: :any)
      {:ok, _, _} = Journey.get(execution, :send_follow_up, wait: :any)

      result = Journey.Tools.introspect(execution.id)

      # Get actual system node values for expected output
      values = Journey.values(execution)
      last_updated_at_value = Map.get(values, :last_updated_at)
      execution_id_value = Map.get(values, :execution_id)

      # Redact dynamic values for reliable comparison
      redacted_result =
        result
        |> redact_text_timestamps()
        |> redact_text_duration()
        |> redact_text_seconds_ago()
        |> String.replace(~r/CMP[A-Z0-9]+/, "CMPREDACTED")

      # Complete expected output as living documentation for engineers
      # This shows how :not conditions are formatted in the summary output
      expected_output = """
      Execution summary:
      - ID: '#{execution.id}'
      - Graph: '#{graph.name}' | 'v1.0.0'
      - Archived at: not archived
      - Created at: REDACTED UTC | REDACTED seconds ago
      - Last updated at: REDACTED UTC | REDACTED seconds ago
      - Duration: REDACTED seconds
      - Revision: 4
      - # of Values: 4 (set) / 12 (total)
      - # of Computations: 5

      Values:
      - Set:
        - last_updated_at: '#{last_updated_at_value}' | :input
          set at REDACTED | rev: 4

        - send_follow_up: '\"Follow up message\"' | :compute
          computed at REDACTED | rev: 4

        - send_reminder: '\"Please request your card\"' | :compute
          computed at REDACTED | rev: 3

        - execution_id: '#{execution_id_value}' | :input
          set at REDACTED | rev: 0


      - Not set:
        - card_mailed: <unk> | :input
        - send_approval_notice: <unk> | :compute
        - send_welcome: <unk> | :compute
        - user_applied: <unk> | :input
        - user_applied_or_card_mailed: <unk> | :compute
        - user_approved: <unk> | :input
        - user_name: <unk> | :input
        - user_requested_card: <unk> | :input  

      Computations:
      - Completed:
        - :send_follow_up (CMPREDACTED): ✅ :success | :compute | rev 4
          inputs used: 
             :user_applied (rev 0)
             :card_mailed (rev 0)
        - :send_reminder (CMPREDACTED): ✅ :success | :compute | rev 3
          inputs used: 
             :user_requested_card (rev 0)

      - Outstanding:
        - user_applied_or_card_mailed: ⬜ :not_set (not yet attempted) | :compute
             :or
              ├─ 🛑 :user_applied | &true?/1
              └─ 🛑 :card_mailed | &true?/1
        - send_welcome: ⬜ :not_set (not yet attempted) | :compute
             🛑 :user_name | &provided?/1
        - send_approval_notice: ⬜ :not_set (not yet attempted) | :compute
             :and
              ├─ 🛑 :user_applied | &true?/1
              ├─ 🛑 :user_approved | &true?/1
              └─ 🛑 :not(:user_requested_card) | &true?/1
      """

      # The test has a race condition: send_follow_up and send_reminder can complete
      # in either order since they have no dependencies on each other. Both orderings
      # are valid. We need to accept either:
      # 1. send_reminder (rev 4), send_follow_up (rev 3), OR
      # 2. send_follow_up (rev 4), send_reminder (rev 3)

      # Create alternative expected output with reversed ordering
      expected_output_alt = """
      Execution summary:
      - ID: '#{execution.id}'
      - Graph: '#{graph.name}' | 'v1.0.0'
      - Archived at: not archived
      - Created at: REDACTED UTC | REDACTED seconds ago
      - Last updated at: REDACTED UTC | REDACTED seconds ago
      - Duration: REDACTED seconds
      - Revision: 4
      - # of Values: 4 (set) / 12 (total)
      - # of Computations: 5

      Values:
      - Set:
        - last_updated_at: '#{last_updated_at_value}' | :input
          set at REDACTED | rev: 4

        - send_reminder: '\"Please request your card\"' | :compute
          computed at REDACTED | rev: 4

        - send_follow_up: '\"Follow up message\"' | :compute
          computed at REDACTED | rev: 3

        - execution_id: '#{execution_id_value}' | :input
          set at REDACTED | rev: 0


      - Not set:
        - card_mailed: <unk> | :input
        - send_approval_notice: <unk> | :compute
        - send_welcome: <unk> | :compute
        - user_applied: <unk> | :input
        - user_applied_or_card_mailed: <unk> | :compute
        - user_approved: <unk> | :input
        - user_name: <unk> | :input
        - user_requested_card: <unk> | :input  

      Computations:
      - Completed:
        - :send_reminder (CMPREDACTED): ✅ :success | :compute | rev 4
          inputs used: 
             :user_requested_card (rev 0)
        - :send_follow_up (CMPREDACTED): ✅ :success | :compute | rev 3
          inputs used: 
             :user_applied (rev 0)
             :card_mailed (rev 0)

      - Outstanding:
        - user_applied_or_card_mailed: ⬜ :not_set (not yet attempted) | :compute
             :or
              ├─ 🛑 :user_applied | &true?/1
              └─ 🛑 :card_mailed | &true?/1
        - send_welcome: ⬜ :not_set (not yet attempted) | :compute
             🛑 :user_name | &provided?/1
        - send_approval_notice: ⬜ :not_set (not yet attempted) | :compute
             :and
              ├─ 🛑 :user_applied | &true?/1
              ├─ 🛑 :user_approved | &true?/1
              └─ 🛑 :not(:user_requested_card) | &true?/1
      """

      # Accept either ordering as valid
      assert redacted_result in [
               String.trim(expected_output),
               String.trim(expected_output_alt)
             ]
    end
  end

  describe "introspect/1" do
    test "returns formatted text summary" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      result =
        Journey.Tools.introspect(execution.id)
        |> redact_text_timestamps()
        |> redact_text_duration()
        |> redact_text_seconds_ago()

      # Verify it returns a formatted string with expected content
      assert is_binary(result)
      assert result =~ "Execution summary:"
      assert result =~ execution.id
      assert result =~ "test graph 1 Elixir.Journey.Test.Support"
      assert result =~ "- Graph:"
      assert result =~ "- Revision:"
      assert result =~ "Values:"
      assert result =~ "Computations:"

      # Test deprecated functions return the same result
      # Using string-based call to avoid compile-time deprecation warning in test
      deprecated_result_summarize_as_text =
        Code.eval_string("Journey.Tools.summarize_as_text(\"#{execution.id}\")")
        |> elem(0)
        |> redact_text_timestamps()
        |> redact_text_duration()
        |> redact_text_seconds_ago()

      deprecated_result_summarize =
        Code.eval_string("Journey.Tools.summarize(\"#{execution.id}\")")
        |> elem(0)
        |> redact_text_timestamps()
        |> redact_text_duration()
        |> redact_text_seconds_ago()

      assert deprecated_result_summarize_as_text == result
      assert deprecated_result_summarize == result
    end
  end

  describe "generate_mermaid_graph/2" do
    test "backward compatibility - with legend (for existing behavior)" do
      graph = Journey.Test.Support.create_test_graph1()

      mermaid_graph =
        Journey.Tools.generate_mermaid_graph(graph, include_legend: true, include_timestamp: true)
        |> String.split("\n")
        |> Enum.filter(fn line ->
          !String.contains?(line, "Generated at")
        end)
        |> Enum.join("\n")

      assert mermaid_graph ==
               "graph TD\n    %% Graph\n    subgraph Graph[\"🧩 'test graph 1 Elixir.Journey.Test.Support', version 1.0.0\"]\n        execution_id[execution_id]\n        last_updated_at[last_updated_at]\n        user_name[user_name]\n        greeting[\"greeting<br/>(anonymous fn)\"]\n        time_to_issue_reminder_schedule[\"time_to_issue_reminder_schedule<br/>(anonymous fn)<br/>tick_once node\"]\n        reminder[\"reminder<br/>(anonymous fn)\"]\n\n        user_name -->  greeting\n        greeting -->  time_to_issue_reminder_schedule\n        time_to_issue_reminder_schedule -->  reminder\n    end\n\n    %% Legend\n    subgraph Legend[\"📖 Legend\"]\n        LegendInput[\"Input Node<br/>User-provided data\"]\n        LegendCompute[\"Compute Node<br/>Self-computing value\"]\n        LegendSchedule[\"Schedule Node<br/>Scheduled trigger\"]\n        LegendMutate[\"Mutate Node<br/>Mutates the value of another node\"]\n    end\n\n    %% Caption\n\n    %% Styling\n    classDef inputNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000000\n    classDef computeNode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000000\n    classDef scheduleNode fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000000\n    classDef mutateNode fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px,color:#000000\n\n    %% Apply styles to legend nodes\n    class LegendInput inputNode\n    class LegendCompute computeNode\n    class LegendSchedule scheduleNode\n    class LegendMutate mutateNode\n\n    %% Apply styles to actual nodes\n    class user_name,last_updated_at,execution_id inputNode\n    class reminder,greeting computeNode\n    class time_to_issue_reminder_schedule scheduleNode"
    end

    test "default - flow only (no legend, no timestamp)" do
      graph = Journey.Test.Support.create_test_graph1()

      mermaid_graph = Journey.Tools.generate_mermaid_graph(graph)

      # Should not contain legend
      refute mermaid_graph =~ "Legend["
      refute mermaid_graph =~ "LegendInput"

      # Should not contain timestamp
      refute mermaid_graph =~ "Generated at"

      # Should contain the main graph
      assert mermaid_graph =~ "graph TD"
      assert mermaid_graph =~ "subgraph Graph["
      assert mermaid_graph =~ "user_name"
      assert mermaid_graph =~ "greeting"
      assert mermaid_graph =~ "reminder"
    end

    test "with include_legend: true only" do
      graph = Journey.Test.Support.create_test_graph1()

      mermaid_graph = Journey.Tools.generate_mermaid_graph(graph, include_legend: true)

      # Should contain legend
      assert mermaid_graph =~ "Legend["
      assert mermaid_graph =~ "LegendInput"
      assert mermaid_graph =~ "LegendCompute"

      # Should not contain timestamp
      refute mermaid_graph =~ "Generated at"

      # Should contain the main graph
      assert mermaid_graph =~ "graph TD"
      assert mermaid_graph =~ "subgraph Graph["
    end

    test "with include_timestamp: true only" do
      graph = Journey.Test.Support.create_test_graph1()

      mermaid_graph = Journey.Tools.generate_mermaid_graph(graph, include_timestamp: true)

      # Should not contain legend
      refute mermaid_graph =~ "Legend["
      refute mermaid_graph =~ "LegendInput"

      # Should contain timestamp
      assert mermaid_graph =~ "Generated at"
      assert mermaid_graph =~ "UTC"

      # Should contain the main graph
      assert mermaid_graph =~ "graph TD"
      assert mermaid_graph =~ "subgraph Graph["
    end

    test "with both include_legend and include_timestamp" do
      graph = Journey.Test.Support.create_test_graph1()

      mermaid_graph =
        Journey.Tools.generate_mermaid_graph(graph,
          include_legend: true,
          include_timestamp: true
        )

      # Should contain legend
      assert mermaid_graph =~ "Legend["
      assert mermaid_graph =~ "LegendInput"

      # Should contain timestamp
      assert mermaid_graph =~ "Generated at"
      assert mermaid_graph =~ "UTC"

      # Should contain the main graph
      assert mermaid_graph =~ "graph TD"
      assert mermaid_graph =~ "subgraph Graph["
    end

    test "raises error for invalid options" do
      graph = Journey.Test.Support.create_test_graph1()

      assert_raise ArgumentError, fn ->
        Journey.Tools.generate_mermaid_graph(graph, invalid_option: true)
      end

      assert_raise ArgumentError, fn ->
        Journey.Tools.generate_mermaid_graph(graph, include_legend: "not_boolean")
      end
    end
  end

  describe "outstanding_computations/1" do
    test "basic validation" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      ocs = Journey.Tools.outstanding_computations(execution.id)
      assert Enum.count(ocs) == 3

      Enum.each(ocs, fn %{computation: computation} = oc ->
        case computation.node_name do
          :greeting ->
            assert computation.state == :not_set
            assert oc.conditions_met == []
            assert Enum.count(oc.conditions_not_met) == 1

          :reminder ->
            assert computation.state == :not_set
            assert oc.conditions_met == []
            assert Enum.count(oc.conditions_not_met) == 1

          :time_to_issue_reminder_schedule ->
            assert computation.state == :not_set
            assert oc.conditions_met == []
            assert Enum.count(oc.conditions_not_met) == 1
        end
      end)

      execution =
        execution
        |> Journey.set(:user_name, "John Doe")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _, _} = Journey.get(execution, :reminder, wait: :any)

      ocs = Journey.Tools.outstanding_computations(execution.id)
      assert ocs == []

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end

  describe "retry_computation/2" do
    test "allows retrying failed computations after max_retries exhausted" do
      # Create a graph with a computation that can fail
      graph =
        Journey.new_graph("retry test #{random_string()}", "v1", [
          input(:trigger),
          compute(
            :failing_computation,
            [:trigger],
            fn _inputs ->
              {:error, "Simulated failure"}
            end,
            # Low retry limit for testing
            max_retries: 2
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :trigger, "start")

      # Start background sweeps to process computations
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Verify computation has failed and won't retry more
      execution = keep_advancing(execution, 2)
      computations = find_computations_by_node_name(execution, :failing_computation)
      assert Enum.all?(computations, fn c -> c.state == :failed end)
      assert length(computations) == 2

      # Use retry_computation
      execution = Journey.Tools.retry_computation(execution.id, :failing_computation)

      # Verify the new computations have been attempted, and also failed.
      execution = keep_advancing(execution, 3)
      computations = find_computations_by_node_name(execution, :failing_computation)
      assert Enum.all?(computations, fn c -> c.state == :failed end)
      assert length(computations) == 3

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    defp keep_advancing(execution, remaining_count) when remaining_count == 0 do
      :timer.sleep(1000)
      Journey.load(execution)
    end

    defp keep_advancing(execution, remaining_count) when remaining_count > 0 do
      :timer.sleep(1000)

      execution
      |> Journey.Scheduler.advance()
      |> keep_advancing(remaining_count - 1)
    end
  end

  describe "abandon_computation/1" do
    test "abandons a computing computation and schedules retry" do
      # Create a graph with a slow computation that will stay in :computing state
      graph =
        Journey.new_graph("abandon test #{random_string()}", "v1", [
          input(:trigger),
          compute(
            :slow_computation,
            [:trigger],
            fn _inputs ->
              # Long sleep to ensure we can catch it in :computing state
              Process.sleep(:timer.seconds(30))
              {:ok, "done"}
            end,
            abandon_after_seconds: 60,
            max_retries: 3
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :trigger, "start")

      # Start background sweeps to trigger the computation
      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Wait for computation to enter :computing state (polls until found or timeout)
      {:ok, computing_computation} =
        wait_for_computation_state(execution, :slow_computation, :computing)

      assert computing_computation != nil

      # Abandon the computation
      {:ok, abandoned_computation} = Journey.Tools.abandon_computation(computing_computation.id)

      # Verify the computation is now abandoned
      assert abandoned_computation.state == :abandoned
      assert abandoned_computation.completion_time != nil

      # Verify a retry was scheduled (should have 2 computations now - original abandoned + new retry)
      execution = Journey.load(execution.id)
      computations = find_computations_by_node_name(execution, :slow_computation)

      # Should have at least 2 computations: the original (now abandoned) and the retry
      assert length(computations) >= 2, "Expected retry computation to be created"

      # Original should be abandoned
      assert Enum.any?(computations, fn c -> c.state == :abandoned end),
             "Expected original computation to be abandoned"

      # Retry should exist (either :not_set if not picked up yet, or :computing if already picked up)
      assert Enum.any?(computations, fn c -> c.state in [:not_set, :computing] end),
             "Expected retry computation to be scheduled"

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "returns error for non-existent computation" do
      result = Journey.Tools.abandon_computation("CMP_NONEXISTENT_ID")
      assert result == {:error, :not_found}
    end

    test "returns error for computation not in :computing state" do
      # Create a graph with a fast computation
      graph =
        Journey.new_graph("abandon state test #{random_string()}", "v1", [
          input(:trigger),
          compute(
            :fast_computation,
            [:trigger],
            fn _inputs -> {:ok, "done"} end
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :trigger, "start")

      # Start background sweeps and wait for completion
      background_sweeps_task = start_background_sweeps_in_test(execution.id)
      {:ok, _, _} = Journey.get(execution, :fast_computation, wait: :any)

      # Find the completed computation
      execution = Journey.load(execution.id)

      completed_computation =
        find_computations_by_node_name(execution, :fast_computation)
        |> Enum.find(fn c -> c.state == :success end)

      assert completed_computation != nil

      # Try to abandon it - should fail since it's already :success
      result = Journey.Tools.abandon_computation(completed_computation.id)
      assert result == {:error, :invalid_state, :success}

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "does not schedule retry when max_retries exhausted" do
      # With max_retries: 0, no retries are allowed - abandoning should not schedule a retry
      graph =
        Journey.new_graph("abandon no retry test #{random_string()}", "v1", [
          input(:trigger),
          compute(
            :limited_retry_computation,
            [:trigger],
            fn _inputs ->
              Process.sleep(:timer.seconds(30))
              {:ok, "done"}
            end,
            abandon_after_seconds: 60,
            max_retries: 0
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :trigger, "start")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      # Wait for computation to enter :computing state
      {:ok, computing} =
        wait_for_computation_state(execution, :limited_retry_computation, :computing)

      assert computing != nil

      # Abandon - should NOT schedule retry since max_retries: 0
      {:ok, _} = Journey.Tools.abandon_computation(computing.id)

      # Verify no retry was scheduled (only the one abandoned computation exists)
      execution = Journey.load(execution.id)
      computations = find_computations_by_node_name(execution, :limited_retry_computation)

      assert length(computations) == 1, "Expected no retry to be scheduled"
      assert hd(computations).state == :abandoned

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end

  # Helper functions for text redaction in tests
  defp redact_text_timestamps(text) do
    text
    |> String.replace(~r/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}Z/, "REDACTED")
  end

  defp redact_text_duration(text) do
    text
    |> String.replace(~r/Duration: \d+ seconds/, "Duration: REDACTED seconds")
  end

  defp redact_text_seconds_ago(text) do
    text
    |> String.replace(~r/\d+ seconds ago/, "REDACTED seconds ago")
  end

  # Helper to reliably wait for a computation to reach a specific state.
  # Returns {:ok, computation} on success, {:error, :timeout} on timeout.
  defp wait_for_computation_state(execution, node_name, expected_state, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    frequency = Keyword.get(opts, :frequency, 100)

    result =
      wait(
        (fn ->
           execution = Journey.load(execution.id)

           find_computations_by_node_name(execution, node_name)
           |> Enum.find(fn c -> c.state == expected_state end)
         end).(),
        timeout: timeout,
        frequency: frequency
      )

    case result do
      {:error, :timeout} -> {:error, :timeout}
      nil -> {:error, :timeout}
      computation -> {:ok, computation}
    end
  end

  describe "orphaned node handling" do
    # Helper to create an execution with orphaned nodes.
    # Creates graph v1 with 3 nodes, starts an execution, then re-registers
    # the same graph name/version with only 1 node — leaving orphaned computations.
    defp create_execution_with_orphaned_nodes do
      graph_name = "orphaned nodes test #{random_string()}"

      graph_v1 =
        Journey.new_graph(graph_name, "v1", [
          input(:input_a),
          compute(:compute_b, [:input_a], fn %{input_a: a} -> {:ok, a} end),
          compute(:compute_c, [:input_a], fn %{input_a: a} -> {:ok, a} end)
        ])

      execution = Journey.start_execution(graph_v1)

      # Re-register with fewer nodes — compute_b and compute_c are now orphaned
      _graph_v1_reduced =
        Journey.new_graph(graph_name, "v1", [
          input(:input_a)
        ])

      execution
    end

    test "introspect/1 doesn't crash on orphaned nodes" do
      execution = create_execution_with_orphaned_nodes()

      result = Journey.Tools.introspect(execution.id)

      assert is_binary(result)
      assert result =~ "node not found in current graph definition"
    end

    test "what_am_i_waiting_for/2 returns descriptive string for orphaned node" do
      execution = create_execution_with_orphaned_nodes()

      result = Journey.Tools.what_am_i_waiting_for(execution.id, :compute_b)

      assert result == "Node :compute_b not found in graph"
    end

    test "computation_state/2 returns :node_not_found for orphaned node" do
      execution = create_execution_with_orphaned_nodes()

      assert Journey.Tools.computation_state(execution.id, :compute_b) == :node_not_found
    end

    test "outstanding_computations/1 filters out orphaned nodes" do
      execution = create_execution_with_orphaned_nodes()

      result = Journey.Tools.outstanding_computations(execution.id)

      # Orphaned nodes should be silently filtered out
      node_names = Enum.map(result, fn %{computation: c} -> c.node_name end)
      refute :compute_b in node_names
      refute :compute_c in node_names
    end

    test "retry_computation/2 returns {:error, :node_not_found} for orphaned node" do
      execution = create_execution_with_orphaned_nodes()

      assert Journey.Tools.retry_computation(execution.id, :compute_b) == {:error, :node_not_found}
    end
  end
end
