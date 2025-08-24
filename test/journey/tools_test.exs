defmodule Journey.ToolsTest do
  use ExUnit.Case, async: true

  import Journey.Scheduler.Background.Periodic,
    only: [start_background_sweeps_in_test: 1, stop_background_sweeps_in_test: 1]

  describe "summarize/1" do
    test "basic validation" do
      graph = Journey.Test.Support.create_test_graph1()
      execution = Journey.start_execution(graph)

      summary = Journey.Tools.summarize(execution.id)

      assert summary =~ """
             Execution summary:
             - ID: '#{execution.id}'
             - Graph: 'test graph 1 Elixir.Journey.Test.Support' | '1.0.0'
             - Archived at: not archived
             """
    end

    test "no such execution" do
      graph = Journey.Test.Support.create_test_graph1()
      _execution = Journey.start_execution(graph)

      assert_raise KeyError, fn ->
        Journey.Tools.summarize("none such")
      end
    end

    test "basic validation, progressed execution " do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set_value(:user_name, "John Doe")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _} = Journey.get_value(execution, :reminder, wait_any: true)

      summary = Journey.Tools.summarize(execution.id)

      assert summary =~ """
             Execution summary:
             - ID: '#{execution.id}'
             - Graph: 'test graph 1 Elixir.Journey.Test.Support' | '1.0.0'
             - Archived at: not archived
             """

      stop_background_sweeps_in_test(background_sweeps_task)
    end

    test "uses emoji format for computation states" do
      import Journey.Node

      # Create a graph with multiple compute nodes
      graph =
        Journey.new_graph("emoji test graph", "v1.0.0", [
          input(:value),
          compute(:success_node, [:value], fn %{value: v} -> {:ok, v * 2} end),
          compute(:fail_node, [:value], fn _deps -> {:error, "intentional failure"} end)
        ])

      execution = Journey.start_execution(graph)

      # Check initial state shows outstanding computations
      summary_initial = Journey.Tools.summarize(execution.id)
      assert summary_initial =~ "⬜ :not_set (not yet attempted)"
      assert summary_initial =~ "🛑 :value | &provided?/1"

      # Set value to trigger computations
      execution = Journey.set_value(execution, :value, 10)

      # Get values to trigger computations
      {:ok, _} = Journey.get_value(execution, :success_node, wait_new: true)
      # The fail_node will fail
      {:error, _} = Journey.get_value(execution, :fail_node, wait_new: true)

      # Check that completed states use emoji format
      summary_after = Journey.Tools.summarize(execution.id)

      # Should see success emoji for successful computation
      assert summary_after =~ "✅ :success"

      # Should see failure emoji for failed computation
      assert summary_after =~ "❌ :failed"

      # Verify the computation state text helper is being used properly
      assert Journey.Tools.computation_state_to_text(:success) == "✅ :success"
      assert Journey.Tools.computation_state_to_text(:failed) == "❌ :failed"
      assert Journey.Tools.computation_state_to_text(:not_set) == "⬜ :not_set (not yet attempted)"
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
               "graph TD\n    %% Graph\n    subgraph Graph[\"🧩 'test graph 1 Elixir.Journey.Test.Support', version 1.0.0\"]\n        execution_id[execution_id]\n        last_updated_at[last_updated_at]\n        user_name[user_name]\n        greeting[\"greeting<br/>(anonymous fn)\"]\n        time_to_issue_reminder_schedule[\"time_to_issue_reminder_schedule<br/>(anonymous fn)<br/>schedule_once node\"]\n        reminder[\"reminder<br/>(anonymous fn)\"]\n\n        user_name -->  greeting\n        greeting -->  time_to_issue_reminder_schedule\n        greeting -->  reminder\n        time_to_issue_reminder_schedule -->  reminder\n    end\n\n    %% Legend\n    subgraph Legend[\"📖 Legend\"]\n        LegendInput[\"Input Node<br/>User-provided data\"]\n        LegendCompute[\"Compute Node<br/>Self-computing value\"]\n        LegendSchedule[\"Schedule Node<br/>Scheduled trigger\"]\n        LegendMutate[\"Mutate Node<br/>Mutates the value of another node\"]\n    end\n\n    %% Caption\n\n    %% Styling\n    classDef inputNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000000\n    classDef computeNode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000000\n    classDef scheduleNode fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000000\n    classDef mutateNode fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px,color:#000000\n\n    %% Apply styles to legend nodes\n    class LegendInput inputNode\n    class LegendCompute computeNode\n    class LegendSchedule scheduleNode\n    class LegendMutate mutateNode\n\n    %% Apply styles to actual nodes\n    class user_name,last_updated_at,execution_id inputNode\n    class reminder,greeting computeNode\n    class time_to_issue_reminder_schedule scheduleNode"
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

  describe "increment_revision/1" do
    test "sunny day" do
      graph = Journey.Test.Support.create_test_graph1()

      execution =
        Journey.start_execution(graph)
        |> Journey.set_value(:user_name, "John Doe")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _} = Journey.get_value(execution, :reminder, wait_any: true)
      execution = execution |> Journey.load()
      assert execution.revision == 7

      Journey.Tools.increment_revision(execution.id, :user_name)
      {:ok, _} = Journey.get_value(execution, :reminder, wait_new: true)
      execution = execution |> Journey.load()
      assert execution.revision == 12

      stop_background_sweeps_in_test(background_sweeps_task)
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
            assert Enum.count(oc.conditions_not_met) == 2

          :time_to_issue_reminder_schedule ->
            assert computation.state == :not_set
            assert oc.conditions_met == []
            assert Enum.count(oc.conditions_not_met) == 1
        end
      end)

      execution =
        execution
        |> Journey.set_value(:user_name, "John Doe")

      background_sweeps_task = start_background_sweeps_in_test(execution.id)

      {:ok, _} = Journey.get_value(execution, :reminder, wait_any: true)

      ocs = Journey.Tools.outstanding_computations(execution.id)
      assert ocs == []

      stop_background_sweeps_in_test(background_sweeps_task)
    end
  end
end
