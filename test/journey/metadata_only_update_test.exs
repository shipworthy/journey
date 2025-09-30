defmodule Journey.MetadataOnlyUpdateTest do
  use ExUnit.Case, async: true
  import Journey.Node

  describe "metadata-only updates (value unchanged)" do
    test "single value: updating metadata without changing value triggers update" do
      graph =
        Journey.new_graph("metadata-only update - single", "v1.0.0", [
          input(:title)
        ])

      execution = Journey.start_execution(graph)

      # Set initial value with metadata
      execution = Journey.set(execution, :title, "Constant Title", metadata: %{"author_id" => "user1"})
      {:ok, %{value: value1, metadata: metadata1, revision: revision1}} = Journey.get(execution, :title)

      assert value1 == "Constant Title"
      assert metadata1 == %{"author_id" => "user1"}

      # Update metadata only (same value)
      execution = Journey.set(execution, :title, "Constant Title", metadata: %{"author_id" => "user2"})
      {:ok, %{value: value2, metadata: metadata2, revision: revision2}} = Journey.get(execution, :title)

      # Value should be unchanged
      assert value2 == "Constant Title"

      # Metadata should be updated
      assert metadata2 == %{"author_id" => "user2"}

      # Revision should have incremented
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
      {:ok, %{value: _value1, metadata: _metadata1, revision: revision1}} = Journey.get(execution, :title)

      # Set same value and same metadata (should be no-op)
      execution = Journey.set(execution, :title, "Title", metadata: %{"author_id" => "user1"})
      {:ok, %{value: _value2, metadata: _metadata2, revision: revision2}} = Journey.get(execution, :title)

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

      {:ok, %{value: _title1, metadata: title_meta1, revision: title_rev1}} = Journey.get(execution, :title)
      {:ok, %{value: _desc1, metadata: desc_meta1, revision: _desc_rev1}} = Journey.get(execution, :description)

      assert title_meta1 == %{"author_id" => "user1"}
      assert desc_meta1 == %{"author_id" => "user1"}

      # Update metadata only (same values)
      execution =
        Journey.set(
          execution,
          %{title: "Title", description: "Desc"},
          metadata: %{"author_id" => "user2"}
        )

      {:ok, %{value: title2, metadata: title_meta2, revision: title_rev2}} = Journey.get(execution, :title)
      {:ok, %{value: desc2, metadata: desc_meta2, revision: _desc_rev2}} = Journey.get(execution, :description)

      # Values should be unchanged
      assert title2 == "Title"
      assert desc2 == "Desc"

      # Metadata should be updated
      assert title_meta2 == %{"author_id" => "user2"}
      assert desc_meta2 == %{"author_id" => "user2"}

      # Revision should have incremented
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

      {:ok, %{value: _title1, metadata: _title_meta1, revision: title_rev1}} = Journey.get(execution, :title)

      # Change title value AND metadata
      execution =
        Journey.set(
          execution,
          %{title: "Title v2", description: "Desc"},
          metadata: %{"author_id" => "user2"}
        )

      {:ok, %{value: title2, metadata: title_meta2, revision: title_rev2}} = Journey.get(execution, :title)
      {:ok, %{value: desc2, metadata: desc_meta2, revision: _desc_rev2}} = Journey.get(execution, :description)

      # Title value changed
      assert title2 == "Title v2"
      assert title_meta2 == %{"author_id" => "user2"}

      # Description value unchanged but metadata changed
      assert desc2 == "Desc"
      assert desc_meta2 == %{"author_id" => "user2"}

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
      {:ok, %{value: history1, metadata: _, revision: _}} = Journey.get(execution, :content_history, wait: :any)

      assert length(history1) == 1
      assert hd(history1)["value"] == "v1"
      assert hd(history1)["metadata"] == %{"author_id" => "user1"}

      # Metadata-only change (same value)
      execution = Journey.set(execution, :content, "v1", metadata: %{"author_id" => "user2"})
      {:ok, %{value: history2, metadata: _, revision: _}} = Journey.get(execution, :content_history, wait: :newer)

      # Should have TWO entries now
      assert length(history2) == 2
      assert Enum.at(history2, 0)["value"] == "v1"
      assert Enum.at(history2, 0)["metadata"] == %{"author_id" => "user1"}
      assert Enum.at(history2, 1)["value"] == "v1"
      assert Enum.at(history2, 1)["metadata"] == %{"author_id" => "user2"}
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
      {:ok, %{value: history1, metadata: _, revision: _}} = Journey.get(execution, :content_history, wait: :any)

      assert length(history1) == 1

      # Set same value and metadata (no-op)
      execution = Journey.set(execution, :content, "v1", metadata: %{"author_id" => "user1"})

      # Wait a bit to ensure no computation triggered
      Process.sleep(100)

      {:ok, %{value: history2, metadata: _, revision: _}} = Journey.get(execution, :content_history)

      # Should still have only ONE entry
      assert length(history2) == 1
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
      {:ok, %{value: _v1, metadata: meta1, revision: rev1}} = Journey.get(execution, :field)

      assert meta1 == nil

      # Add metadata (same value)
      execution = Journey.set(execution, :field, "value", metadata: %{"added" => "later"})
      {:ok, %{value: v2, metadata: meta2, revision: rev2}} = Journey.get(execution, :field)

      assert v2 == "value"
      assert meta2 == %{"added" => "later"}
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
      {:ok, %{value: _v1, metadata: meta1, revision: rev1}} = Journey.get(execution, :field)

      assert meta1 == %{"author" => "user1"}

      # Remove metadata (same value)
      execution = Journey.set(execution, :field, "value")
      {:ok, %{value: v2, metadata: meta2, revision: rev2}} = Journey.get(execution, :field)

      assert v2 == "value"
      assert meta2 == nil
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

      {:ok, %{value: value, metadata: metadata, revision: _revision}} = Journey.get(execution, :field)
      assert value == "value"
      assert metadata == "simple string"
    end

    test "number metadata works" do
      graph =
        Journey.new_graph("metadata scalar - number", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: 42)

      {:ok, %{value: value, metadata: metadata, revision: _revision}} = Journey.get(execution, :field)
      assert value == "value"
      assert metadata == 42
    end

    test "boolean metadata works" do
      graph =
        Journey.new_graph("metadata scalar - boolean", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: true)

      {:ok, %{value: value, metadata: metadata, revision: _revision}} = Journey.get(execution, :field)
      assert value == "value"
      assert metadata == true
    end

    test "list metadata works" do
      graph =
        Journey.new_graph("metadata scalar - list", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: ["item1", "item2", "item3"])

      {:ok, %{value: value, metadata: metadata, revision: _revision}} = Journey.get(execution, :field)
      assert value == "value"
      assert metadata == ["item1", "item2", "item3"]
    end

    test "string metadata-only update triggers revision" do
      graph =
        Journey.new_graph("metadata scalar - string update", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: "version1")
      {:ok, %{value: _v1, metadata: meta1, revision: rev1}} = Journey.get(execution, :field)

      assert meta1 == "version1"

      # Update only metadata
      execution = Journey.set(execution, :field, "value", metadata: "version2")
      {:ok, %{value: v2, metadata: meta2, revision: rev2}} = Journey.get(execution, :field)

      assert v2 == "value"
      assert meta2 == "version2"
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

      {:ok, %{value: value, metadata: _metadata, revision: _revision}} = Journey.get(execution, :config)
      assert value == %{"retry_count" => 3}
    end

    test "metadata with string keys works" do
      graph =
        Journey.new_graph("validation - metadata string keys", "v1.0.0", [
          input(:field)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :field, "value", metadata: %{"author_id" => "user1"})

      {:ok, %{value: _value, metadata: metadata, revision: _revision}} = Journey.get(execution, :field)
      assert metadata == %{"author_id" => "user1"}
    end
  end
end
