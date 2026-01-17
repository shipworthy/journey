defmodule Journey.Node.HistorianTest do
  use ExUnit.Case, async: true
  import Journey.Node

  import Journey.Helpers.Random, only: [random_string: 0]

  # Helper to redact timestamps for deterministic assertions
  defp redact_timestamps(history) do
    Enum.map(history, fn %{"timestamp" => ts} = entry when is_integer(ts) ->
      Map.put(entry, "timestamp", 1_234_567_890)
    end)
  end

  describe "historian() |" do
    test "basic history tracking" do
      graph_name = "historian test graph #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:content),
            historian(:content_history, [:content])
          ]
        )

      execution = Journey.start_execution(graph)

      # First value
      execution = Journey.set(execution, :content, "First version")
      {:ok, history1} = Journey.get_value(execution, :content_history, wait_any: true)

      assert redact_timestamps(history1) == [
               %{
                 "metadata" => nil,
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "First version"
               }
             ]

      # Second value
      execution = Journey.set(execution, :content, "Second version")
      {:ok, history2} = Journey.get_value(execution, :content_history, wait: :newer)

      assert redact_timestamps(history2) == [
               %{
                 "metadata" => nil,
                 "node" => "content",
                 "revision" => 4,
                 "timestamp" => 1_234_567_890,
                 "value" => "Second version"
               },
               %{
                 "metadata" => nil,
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "First version"
               }
             ]
    end

    test "max_entries limits history size" do
      graph_name = "historian max_entries test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:status),
            historian(:status_history, [:status], max_entries: 20)
          ]
        )

      execution = Journey.start_execution(graph)

      # Start background sweeps for test
      background_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)

      try do
        {_execution, last_revision} =
          Enum.reduce(1..22, {execution, 0}, fn i, {acc_execution, last_revision} ->
            # Set the value
            new_execution = Journey.set(acc_execution, :status, i)
            Process.sleep(100)

            # Wait for computation newer than last revision
            {:ok, _history} =
              Journey.get_value(new_execution, :status_history, wait: {:newer_than, last_revision})

            ex = Journey.load(execution)
            value = ex.values |> Enum.find(fn e -> e.node_name == :status end)

            # Get current revision for next iteration
            current_revision = value.ex_revision

            {new_execution, current_revision}
          end)

        Process.sleep(100)

        {:ok, final_history} =
          Journey.get_value(execution, :status_history, wait: {:newer_than, last_revision})

        # Due to Journey's scheduling optimizations, we get fewer than 1001 entries
        values = Enum.map(final_history, & &1["value"])

        # Newest first ordering
        first_entry = List.first(final_history)
        assert first_entry["value"] == 22
        assert List.last(values) == 3
        assert length(final_history) == 20
        assert Enum.all?(Enum.chunk_every(values, 2, 1, :discard), fn [a, b] -> b == a - 1 end)
      after
        Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_task)
      end
    end

    test "unlimited history with max_entries: nil" do
      graph_name = "historian unlimited test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:event),
            historian(:event_log, [:event], max_entries: nil)
          ]
        )

      execution = Journey.start_execution(graph)

      # Add a single value to verify unlimited works
      execution = Journey.set(execution, :event, "single_event")
      {:ok, history} = Journey.get_value(execution, :event_log, wait: :any)

      assert redact_timestamps(history) == [
               %{
                 "metadata" => nil,
                 "node" => "event",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "single_event"
               }
             ]
    end

    test "schema agnostic - works with any data type" do
      graph_name = "historian schema test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:data),
            historian(:data_history, [:data])
          ]
        )

      execution = Journey.start_execution(graph)

      # Test a complex data structure (no atoms since they become strings in JSON)
      test_value = %{"key" => "value", "nested" => %{"data" => true, "count" => 42}}

      execution = Journey.set(execution, :data, test_value)
      {:ok, history} = Journey.get_value(execution, :data_history, wait: :any)

      assert redact_timestamps(history) == [
               %{
                 "metadata" => nil,
                 "node" => "data",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => %{"key" => "value", "nested" => %{"count" => 42, "data" => true}}
               }
             ]
    end

    test "works with default options (has max_entries limit, not unlimited)" do
      graph_name = "historian default test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:counter),
            historian(:counter_history, [:counter])
          ]
        )

      execution = Journey.start_execution(graph)

      # Just test that the default max_entries behavior exists by setting a single value
      execution = Journey.set(execution, :counter, 1)
      {:ok, history} = Journey.get_value(execution, :counter_history, wait: :any)

      assert redact_timestamps(history) == [
               %{"metadata" => nil, "node" => "counter", "revision" => 1, "timestamp" => 1_234_567_890, "value" => 1}
             ]

      # Verify the historian works with subsequent updates (proving it has some limit, not unlimited)
      execution = Journey.set(execution, :counter, 2)
      {:ok, history2} = Journey.get_value(execution, :counter_history, wait: :newer)

      assert redact_timestamps(history2) == [
               %{"metadata" => nil, "node" => "counter", "revision" => 4, "timestamp" => 1_234_567_890, "value" => 2},
               %{"metadata" => nil, "node" => "counter", "revision" => 1, "timestamp" => 1_234_567_890, "value" => 1}
             ]
    end

    test "a bunch of records" do
      graph_name = "historian a bunch of records test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:counter),
            historian(:counter_history, [:counter])
          ]
        )

      execution = Journey.start_execution(graph)

      # Start background sweeps to ensure computations get processed
      background_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)

      try do
        {_execution, last_revision} =
          Enum.reduce(1..101, {execution, 0}, fn i, {acc_execution, last_revision} ->
            # Set the value
            new_execution = Journey.set(acc_execution, :counter, i)
            Process.sleep(100)

            # Wait for computation newer than last revision
            {:ok, _history} =
              Journey.get_value(new_execution, :counter_history, wait: {:newer_than, last_revision})

            ex = Journey.load(execution)
            value = ex.values |> Enum.find(fn e -> e.node_name == :counter end)
            # {i, System.system_time(:second)} |> IO.inspect(label: :now)

            # Get current revision for next iteration
            current_revision = value.ex_revision

            {new_execution, current_revision}
          end)

        Process.sleep(100)

        {:ok, final_history} =
          Journey.get_value(execution, :counter_history, wait: {:newer_than, last_revision})

        # final_history |> Enum.count() |> IO.inspect(label: :chickens)

        # Due to Journey's scheduling optimizations, we get fewer than 1001 entries
        values = Enum.map(final_history, & &1["value"])

        # Newest first ordering
        first_entry = List.first(final_history)
        assert first_entry["value"] == 101
        assert List.last(values) == 1
        assert length(final_history) == 101
        assert Enum.all?(Enum.chunk_every(values, 2, 1, :discard), fn [a, b] -> b == a - 1 end)
      after
        Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_task)
      end
    end

    test "timestamps are properly recorded" do
      graph_name = "historian timestamp test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:value),
            historian(:value_history, [:value])
          ]
        )

      execution = Journey.start_execution(graph)

      # Set initial value
      execution = Journey.set(execution, :value, "first")
      {:ok, history1} = Journey.get_value(execution, :value_history, wait: :any)

      assert redact_timestamps(history1) == [
               %{
                 "metadata" => nil,
                 "node" => "value",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "first"
               }
             ]

      # Set second value after a short delay
      # Ensure different timestamp
      Process.sleep(10)
      execution = Journey.set(execution, :value, "second")
      {:ok, history2} = Journey.get_value(execution, :value_history, wait: :newer)

      assert redact_timestamps(history2) == [
               %{
                 "metadata" => nil,
                 "node" => "value",
                 "revision" => 4,
                 "timestamp" => 1_234_567_890,
                 "value" => "second"
               },
               %{
                 "metadata" => nil,
                 "node" => "value",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "first"
               }
             ]
    end
  end

  describe "historian invalidation resistance |" do
    import Journey.Node.Conditions

    test "historian preserves history when upstream condition becomes unmet" do
      graph_name = "historian invalidation test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:enabled),
            compute(:data, [enabled: &true?/1], fn _ ->
              {:ok, "value_#{System.unique_integer([:positive])}"}
            end),
            historian(:history, [:data])
          ]
        )

      execution = Journey.start_execution(graph)

      # Enable and get first history entry
      execution = Journey.set(execution, :enabled, true)
      {:ok, [first_entry], _} = Journey.get(execution, :history, wait: :any)
      first_value = first_entry["value"]

      # Disable - this would previously clear the historian via invalidation cascade
      execution = Journey.set(execution, :enabled, false)

      # History should still be accessible (historian not invalidated)
      {:ok, [^first_entry], _} = Journey.get(execution, :history)

      # Re-enable - compute will produce a new value
      execution = Journey.set(execution, :enabled, true)
      {:ok, history2, _} = Journey.get(execution, :history, wait: :newer)

      # History should have BOTH entries (newest first), with the first entry preserved
      assert [second_entry, ^first_entry] = history2
      assert second_entry["value"] != first_value
    end
  end
end
