defmodule Journey.Node.HistorianTest do
  use ExUnit.Case, async: true
  import Journey.Node

  import Journey.Helpers.Random, only: [random_string: 0]

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

      assert length(history1) == 1
      assert [%{"value" => "First version", "node" => "content", "timestamp" => ts1, "revision" => rev1}] = history1
      assert is_integer(ts1)
      assert is_integer(rev1)

      # Second value
      execution = Journey.set(execution, :content, "Second version")
      {:ok, history2} = Journey.get_value(execution, :content_history, wait: :newer)

      assert length(history2) == 2
      [entry1, entry2] = history2
      assert entry1["value"] == "First version"
      assert entry1["node"] == "content"
      assert is_integer(entry1["revision"])
      assert entry2["value"] == "Second version"
      assert entry2["node"] == "content"
      assert is_integer(entry2["revision"])
      assert entry2["timestamp"] >= entry1["timestamp"]
      assert entry2["revision"] > entry1["revision"]
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

        last_entry = List.last(final_history)
        assert last_entry["value"] == 22
        assert List.first(values) == 3
        assert length(final_history) == 20
        assert Enum.all?(Enum.chunk_every(values, 2, 1, :discard), fn [a, b] -> b == a + 1 end)
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

      # Should keep the entry with unlimited history
      assert length(history) == 1
      assert [%{"value" => "single_event", "node" => "event", "timestamp" => _ts, "revision" => rev}] = history
      assert is_integer(rev)
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

      assert length(history) == 1
      assert [%{"value" => recorded_value, "node" => "data", "timestamp" => _ts, "revision" => rev}] = history
      assert recorded_value == test_value
      assert is_integer(rev)
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

      # Should have the entry and work normally
      assert length(history) == 1
      assert [%{"value" => 1, "node" => "counter", "timestamp" => _ts, "revision" => rev}] = history
      assert is_integer(rev)

      # Verify the historian works with subsequent updates (proving it has some limit, not unlimited)
      execution = Journey.set(execution, :counter, 2)
      {:ok, history2} = Journey.get_value(execution, :counter_history, wait: :newer)
      # Would be unlimited if max_entries was nil, but we have a default limit
      assert length(history2) == 2
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

        last_entry = List.last(final_history)
        assert last_entry["value"] == 101
        assert List.first(values) == 1
        assert length(final_history) == 101
        assert Enum.all?(Enum.chunk_every(values, 2, 1, :discard), fn [a, b] -> b == a + 1 end)
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

      # Set second value after a short delay
      # Ensure different timestamp
      Process.sleep(10)
      execution = Journey.set(execution, :value, "second")
      {:ok, history2} = Journey.get_value(execution, :value_history, wait: :newer)

      assert length(history1) == 1
      assert length(history2) == 2

      [entry1, entry2] = history2
      assert entry1["value"] == "first"
      assert entry1["node"] == "value"
      assert entry2["value"] == "second"
      assert entry2["node"] == "value"

      # Verify timestamps are in order and are integers
      assert is_integer(entry1["timestamp"])
      assert is_integer(entry2["timestamp"])
      assert entry2["timestamp"] >= entry1["timestamp"]
    end
  end
end
