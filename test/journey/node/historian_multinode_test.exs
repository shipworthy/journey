defmodule Journey.Node.HistorianMultinodeTest do
  use ExUnit.Case, async: true
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  import Journey.Helpers.Random, only: [random_string: 0]

  # Helper to redact timestamps for deterministic assertions
  defp redact_timestamps(history) do
    Enum.map(history, fn %{"timestamp" => ts} = entry when is_integer(ts) ->
      Map.put(entry, "timestamp", 1_234_567_890)
    end)
  end

  describe "historian() with multiple nodes |" do
    test "3 nodes" do
      g =
        Journey.new_graph(
          "test graph #{__MODULE__}",
          random_string(),
          [
            input(:a),
            input(:b),
            input(:c),
            historian(
              :abc_history,
              unblocked_when({
                :or,
                [
                  {:a, &provided?/1},
                  {:b, &provided?/1},
                  {:c, &provided?/1}
                ]
              })
            )
          ]
        )

      e = Journey.start_execution(g)

      # Set a - historian should record it
      e = Journey.set(e, :a, "a")
      {:ok, %{value: history1, revision: rev1}} = Journey.get(e, :abc_history, wait: :any)

      # Assert exact structure with redacted timestamps
      assert redact_timestamps(history1) == [
               %{"value" => "a", "node" => "a", "timestamp" => 1_234_567_890, "metadata" => nil, "revision" => 1}
             ]

      # Set b - historian should record it
      Journey.set(e, :b, "b")
      {:ok, %{value: history2, revision: rev2}} = Journey.get(e, :abc_history, wait: {:newer_than, rev1})

      assert redact_timestamps(history2) == [
               %{"metadata" => nil, "node" => "b", "revision" => 4, "timestamp" => 1_234_567_890, "value" => "b"},
               %{"metadata" => nil, "node" => "a", "revision" => 1, "timestamp" => 1_234_567_890, "value" => "a"}
             ]

      # Set c - historian should record it
      Journey.set(e, :c, "c")
      {:ok, %{value: history3, revision: rev3}} = Journey.get(e, :abc_history, wait: {:newer_than, rev2})

      assert redact_timestamps(history3) == [
               %{"metadata" => nil, "node" => "c", "revision" => 7, "timestamp" => 1_234_567_890, "value" => "c"},
               %{"metadata" => nil, "node" => "b", "revision" => 4, "timestamp" => 1_234_567_890, "value" => "b"},
               %{"metadata" => nil, "node" => "a", "revision" => 1, "timestamp" => 1_234_567_890, "value" => "a"}
             ]

      # Set another b - historian should record it
      Journey.set(e, :b, "b1")
      {:ok, %{value: history4, revision: _rev4}} = Journey.get(e, :abc_history, wait: {:newer_than, rev3})

      assert redact_timestamps(history4) == [
               %{"metadata" => nil, "node" => "b", "revision" => 10, "timestamp" => 1_234_567_890, "value" => "b1"},
               %{"metadata" => nil, "node" => "c", "revision" => 7, "timestamp" => 1_234_567_890, "value" => "c"},
               %{"metadata" => nil, "node" => "b", "revision" => 4, "timestamp" => 1_234_567_890, "value" => "b"},
               %{"metadata" => nil, "node" => "a", "revision" => 1, "timestamp" => 1_234_567_890, "value" => "a"}
             ]
    end
  end
end
