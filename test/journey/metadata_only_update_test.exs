defmodule Journey.MetadataOnlyUpdateTest do
  use ExUnit.Case, async: true
  import Journey.Node

  # Helper to redact timestamps for deterministic assertions
  defp redact_timestamps(history) do
    Enum.map(history, fn %{"timestamp" => ts} = entry when is_integer(ts) ->
      Map.put(entry, "timestamp", 1_234_567_890)
    end)
  end

  describe "metadata-only updates (value unchanged)" do
    test "single value: updating metadata without changing value triggers update" do
      graph =
        Journey.new_graph("metadata-only update - single", "v1.0.0", [
          input(:title)
        ])

      execution = Journey.start_execution(graph)

      # Set initial value with metadata
      execution = Journey.set(execution, :title, "Constant Title", metadata: %{"author_id" => "user1"})
      {:ok, value1, revision1} = Journey.get(execution, :title)

      assert value1 == "Constant Title"

      # Update metadata only (same value)
      execution = Journey.set(execution, :title, "Constant Title", metadata: %{"author_id" => "user2"})
      {:ok, value2, revision2} = Journey.get(execution, :title)

      # Value should be unchanged
      assert value2 == "Constant Title"

      # Revision should have incremented (metadata-only update)
      assert revision2 > revision1
    end

    test "single value: setting same value and same metadata is a no-op" do
      graph =
        Journey.new_graph("metadata-only update - no-op", "v1.0.0", [
          input(:title)
        ])

      execution = Journey.start_execution(graph)

      # Set initial value with metadata
      execution = Journey.set(execution, :title, "Title", metadata: %{"author_id" => "user1"})
      {:ok, _value1, revision1} = Journey.get(execution, :title)

      # Set same value and same metadata (should be no-op)
      execution = Journey.set(execution, :title, "Title", metadata: %{"author_id" => "user1"})
      {:ok, _value2, revision2} = Journey.get(execution, :title)

      # Revision should NOT increment
      assert revision2 == revision1
    end

    test "bulk set: updating metadata without changing values triggers update" do
      graph =
        Journey.new_graph("metadata-only update - bulk", "v1.0.0", [
          input(:title),
          input(:description)
        ])

      execution = Journey.start_execution(graph)

      # Set initial values with metadata
      execution =
        Journey.set(
          execution,
          %{title: "Title", description: "Desc"},
          metadata: %{"author_id" => "user1"}
        )

      {:ok, _title1, title_rev1} = Journey.get(execution, :title)
      {:ok, _desc1, _desc_rev1} = Journey.get(execution, :description)

      # Update metadata only (same values)
      execution =
        Journey.set(
          execution,
          %{title: "Title", description: "Desc"},
          metadata: %{"author_id" => "user2"}
        )

      {:ok, title2, title_rev2} = Journey.get(execution, :title)
      {:ok, desc2, _desc_rev2} = Journey.get(execution, :description)

      # Values should be unchanged
      assert title2 == "Title"
      assert desc2 == "Desc"

      # Revision should have incremented (metadata-only update)
      assert title_rev2 > title_rev1
    end

    test "bulk set: mixed scenario (one value changes, one metadata changes)" do
      graph =
        Journey.new_graph("metadata-only update - mixed", "v1.0.0", [
          input(:title),
          input(:description)
        ])

      execution = Journey.start_execution(graph)

      # Set initial values with metadata
      execution =
        Journey.set(
          execution,
          %{title: "Title v1", description: "Desc"},
          metadata: %{"author_id" => "user1"}
        )

      {:ok, _title1, title_rev1} = Journey.get(execution, :title)

      # Change title value AND metadata
      execution =
        Journey.set(
          execution,
          %{title: "Title v2", description: "Desc"},
          metadata: %{"author_id" => "user2"}
        )

      {:ok, title2, title_rev2} = Journey.get(execution, :title)
      {:ok, desc2, _desc_rev2} = Journey.get(execution, :description)

      # Title value changed
      assert title2 == "Title v2"

      # Description value unchanged
      assert desc2 == "Desc"

      # Revision incremented
      assert title_rev2 > title_rev1
    end
  end

  describe "historian captures metadata-only updates" do
    test "historian creates new entry when only metadata changes" do
      graph =
        Journey.new_graph("metadata-only update - historian", "v1.0.0", [
          input(:content),
          historian(:content_history, [:content])
        ])

      execution = Journey.start_execution(graph)

      # First change
      execution = Journey.set(execution, :content, "v1", metadata: %{"author_id" => "user1"})
      {:ok, history1, rev1} = Journey.get(execution, :content_history, wait: :any)

      assert redact_timestamps(history1) == [
               %{
                 "metadata" => %{"author_id" => "user1"},
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               }
             ]

      # Metadata-only change (same value)
      execution = Journey.set(execution, :content, "v1", metadata: %{"author_id" => "user2"})

      {:ok, history2, _rev2} = Journey.get(execution, :content_history, wait: {:newer_than, rev1})

      # Should have TWO entries now (newest first: user2, then user1)
      assert redact_timestamps(history2) == [
               %{
                 "metadata" => %{"author_id" => "user2"},
                 "node" => "content",
                 "revision" => 4,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               },
               %{
                 "metadata" => %{"author_id" => "user1"},
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               }
             ]
    end

    test "historian does not create duplicate entry when value and metadata unchanged" do
      graph =
        Journey.new_graph("metadata-only update - historian no-op", "v1.0.0", [
          input(:content),
          historian(:content_history, [:content])
        ])

      execution = Journey.start_execution(graph)

      # First change
      execution = Journey.set(execution, :content, "v1", metadata: %{"author_id" => "user1"})
      {:ok, history1, _} = Journey.get(execution, :content_history, wait: :any)

      assert redact_timestamps(history1) == [
               %{
                 "metadata" => %{"author_id" => "user1"},
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               }
             ]

      # Set same value and metadata (no-op)
      execution = Journey.set(execution, :content, "v1", metadata: %{"author_id" => "user1"})

      # Wait a bit to ensure no computation triggered
      Process.sleep(100)

      {:ok, history2, _} = Journey.get(execution, :content_history)

      # Should still have only ONE entry (no new revision)
      assert redact_timestamps(history2) == [
               %{
                 "metadata" => %{"author_id" => "user1"},
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               }
             ]
    end
  end

  describe "edge cases" do
    test "updating from nil metadata to non-nil metadata triggers update" do
      graph =
        Journey.new_graph("metadata-only update - nil to value", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)

      # Set value without metadata
      execution = Journey.set(execution, :field, "value")
      {:ok, _v1, rev1} = Journey.get(execution, :field)

      # Add metadata (same value)
      execution = Journey.set(execution, :field, "value", metadata: %{"added" => "later"})
      {:ok, v2, rev2} = Journey.get(execution, :field)

      assert v2 == "value"
      # Revision incremented (metadata added)
      assert rev2 > rev1
    end

    test "updating from non-nil metadata to nil metadata triggers update" do
      graph =
        Journey.new_graph("metadata-only update - value to nil", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)

      # Set value with metadata
      execution = Journey.set(execution, :field, "value", metadata: %{"author" => "user1"})
      {:ok, _v1, rev1} = Journey.get(execution, :field)

      # Remove metadata (same value)
      execution = Journey.set(execution, :field, "value")
      {:ok, v2, rev2} = Journey.get(execution, :field)

      assert v2 == "value"
      # Revision incremented (metadata removed)
      assert rev2 > rev1
    end
  end

  describe "scalar metadata types" do
    test "string metadata works" do
      graph =
        Journey.new_graph("metadata scalar - string", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: "simple string")

      {:ok, value, _revision} = Journey.get(execution, :field)
      assert value == "value"
      # Note: metadata is stored internally but not exposed via Journey.get()
    end

    test "number metadata works" do
      graph =
        Journey.new_graph("metadata scalar - number", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: 42)

      {:ok, value, _revision} = Journey.get(execution, :field)
      assert value == "value"
      # Note: metadata is stored internally but not exposed via Journey.get()
    end

    test "boolean metadata works" do
      graph =
        Journey.new_graph("metadata scalar - boolean", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: true)

      {:ok, value, _revision} = Journey.get(execution, :field)
      assert value == "value"
      # Note: metadata is stored internally but not exposed via Journey.get()
    end

    test "list metadata works" do
      graph =
        Journey.new_graph("metadata scalar - list", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: ["item1", "item2", "item3"])

      {:ok, value, _revision} = Journey.get(execution, :field)
      assert value == "value"
      # Note: metadata is stored internally but not exposed via Journey.get()
    end

    test "string metadata-only update triggers revision" do
      graph =
        Journey.new_graph("metadata scalar - string update", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: "version1")
      {:ok, _v1, rev1} = Journey.get(execution, :field)

      # Update only metadata
      execution = Journey.set(execution, :field, "value", metadata: "version2")
      {:ok, v2, rev2} = Journey.get(execution, :field)

      assert v2 == "value"
      # Revision incremented (metadata-only update)
      assert rev2 > rev1
    end
  end

  describe "validation: map keys must be strings" do
    test "map value with atom keys raises ArgumentError" do
      graph =
        Journey.new_graph("validation - map value atom keys", "v1.0.0", [
          input(:config)
        ])

      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, ~r/Map keys must be strings.*Found atom keys: \[:retry_count\]/, fn ->
        Journey.set(execution, :config, %{retry_count: 3})
      end
    end

    test "metadata with atom keys raises ArgumentError" do
      graph =
        Journey.new_graph("validation - metadata atom keys", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)

      assert_raise ArgumentError, ~r/Map keys must be strings.*Found atom keys: \[:author_id\]/, fn ->
        Journey.set(execution, :field, "value", metadata: %{author_id: "user1"})
      end
    end

    test "map value with string keys works" do
      graph =
        Journey.new_graph("validation - map value string keys", "v1.0.0", [
          input(:config)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :config, %{"retry_count" => 3})

      {:ok, value, _revision} = Journey.get(execution, :config)
      assert value == %{"retry_count" => 3}
    end

    test "metadata with string keys works" do
      graph =
        Journey.new_graph("validation - metadata string keys", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: %{"author_id" => "user1"})

      {:ok, value, _revision} = Journey.get(execution, :field)
      assert value == "value"
      # Note: metadata is stored internally but not exposed via Journey.get()
    end
  end
end
