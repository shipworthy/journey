defmodule Journey.MetadataTest do
  use ExUnit.Case, async: true
  import Journey.Node

  # Helper to redact timestamps for deterministic assertions
  defp redact_timestamps(history) do
    Enum.map(history, fn %{"timestamp" => ts} = entry when is_integer(ts) ->
      Map.put(entry, "timestamp", 1_234_567_890)
    end)
  end

  describe "basic metadata functionality" do
    test "set and get value with metadata" do
      graph =
        Journey.new_graph("metadata test - basic", "v1.0.0", [
          input(:title)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :title, "Hello", metadata: %{"author_id" => "user123"})

      {:ok, %{value: value, metadata: metadata}} = Journey.get(execution, :title)
      assert value == "Hello"
      assert metadata == %{"author_id" => "user123"}
    end

    test "set without metadata returns nil metadata" do
      graph = Journey.new_graph("metadata test - no metadata", "v1.0.0", [input(:title)])
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :title, "Hello")

      {:ok, %{metadata: metadata}} = Journey.get(execution, :title)
      assert metadata == nil
    end

    test "bulk set with metadata applies to all values" do
      graph =
        Journey.new_graph("metadata test - bulk", "v1.0.0", [
          input(:title),
          input(:description)
        ])

      execution = Journey.start_execution(graph)

      execution =
        Journey.set(
          execution,
          %{
            title: "Title",
            description: "Desc"
          },
          metadata: %{"author_id" => "user123"}
        )

      {:ok, %{metadata: title_meta}} = Journey.get(execution, :title)
      {:ok, %{metadata: desc_meta}} = Journey.get(execution, :description)

      assert title_meta == %{"author_id" => "user123"}
      assert desc_meta == %{"author_id" => "user123"}
    end
  end

  describe "f_compute/2 receives value node data" do
    test "f_compute/2 receives value node data from upstream nodes" do
      graph =
        Journey.new_graph("metadata test - compute with value nodes", "v1.0.0", [
          input(:title),
          compute(
            :title_with_author,
            [:title],
            fn %{title: title}, value_nodes_map ->
              author = get_in(value_nodes_map, [:title, :metadata, "author_id"])
              {:ok, "#{title} by #{author}"}
            end
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :title, "Hello", metadata: %{"author_id" => "user123"})

      {:ok, %{value: result}} = Journey.get(execution, :title_with_author, wait: :any)
      assert result == "Hello by user123"
    end

    test "f_compute/1 still works without value node data (backward compatibility)" do
      graph =
        Journey.new_graph("metadata test - compute without value nodes", "v1.0.0", [
          input(:name),
          compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello #{name}"} end)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :name, "Alice", metadata: %{"author_id" => "user123"})

      {:ok, %{value: result}} = Journey.get(execution, :greeting, wait: :any)
      assert result == "Hello Alice"
    end

    test "f_compute/2 with multiple upstream nodes receives all value node data" do
      graph =
        Journey.new_graph("metadata test - multiple upstream", "v1.0.0", [
          input(:first_name),
          input(:last_name),
          compute(
            :full_name_with_authors,
            [:first_name, :last_name],
            fn %{first_name: first, last_name: last}, value_nodes_map ->
              first_author = get_in(value_nodes_map, [:first_name, :metadata, "author_id"]) || "unknown"
              last_author = get_in(value_nodes_map, [:last_name, :metadata, "author_id"]) || "unknown"
              {:ok, "#{first} #{last} (first by #{first_author}, last by #{last_author})"}
            end
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :first_name, "John", metadata: %{"author_id" => "user1"})
      execution = Journey.set(execution, :last_name, "Doe", metadata: %{"author_id" => "user2"})

      {:ok, %{value: result}} = Journey.get(execution, :full_name_with_authors, wait: :any)
      assert result == "John Doe (first by user1, last by user2)"
    end
  end

  describe "historian includes metadata" do
    test "historian includes metadata in history entries" do
      graph =
        Journey.new_graph("metadata test - historian", "v1.0.0", [
          input(:content),
          historian(:content_history, [:content])
        ])

      execution = Journey.start_execution(graph)

      # First change
      execution = Journey.set(execution, :content, "v1", metadata: %{"author_id" => "user123"})
      {:ok, %{value: history1, revision: rev1}} = Journey.get(execution, :content_history, wait: :any)

      assert redact_timestamps(history1) == [
               %{
                 "metadata" => %{"author_id" => "user123"},
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               }
             ]

      # Second change
      execution = Journey.set(execution, :content, "v2", metadata: %{"author_id" => "user456"})
      {:ok, %{value: history2, revision: _rev2}} = Journey.get(execution, :content_history, wait: {:newer_than, rev1})

      # Newest first: v2, then v1
      assert redact_timestamps(history2) == [
               %{
                 "metadata" => %{"author_id" => "user456"},
                 "node" => "content",
                 "revision" => 4,
                 "timestamp" => 1_234_567_890,
                 "value" => "v2"
               },
               %{
                 "metadata" => %{"author_id" => "user123"},
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               }
             ]
    end

    test "historian handles nil metadata" do
      graph =
        Journey.new_graph("metadata test - historian nil", "v1.0.0", [
          input(:content),
          historian(:content_history, [:content])
        ])

      execution = Journey.start_execution(graph)

      # Change without metadata
      execution = Journey.set(execution, :content, "v1")
      {:ok, %{value: history}} = Journey.get(execution, :content_history, wait: :any)

      assert redact_timestamps(history) == [
               %{
                 "metadata" => nil,
                 "node" => "content",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "v1"
               }
             ]
    end

    test "historian tracks bulk set with metadata" do
      graph =
        Journey.new_graph("metadata test - historian bulk set", "v1.0.0", [
          input(:name),
          input(:age),
          historian(:name_history, [:name]),
          historian(:age_history, [:age])
        ])

      execution = Journey.start_execution(graph)

      # Bulk set with metadata
      execution = Journey.set(execution, %{name: "Mark", age: 120}, metadata: %{"author_id" => "user789"})

      # Verify name history
      {:ok, %{value: name_history}} = Journey.get(execution, :name_history, wait: :any)

      assert redact_timestamps(name_history) == [
               %{
                 "metadata" => %{"author_id" => "user789"},
                 "node" => "name",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => "Mark"
               }
             ]

      # Verify age history
      {:ok, %{value: age_history}} = Journey.get(execution, :age_history, wait: :any)

      assert redact_timestamps(age_history) == [
               %{
                 "metadata" => %{"author_id" => "user789"},
                 "node" => "age",
                 "revision" => 1,
                 "timestamp" => 1_234_567_890,
                 "value" => 120
               }
             ]
    end
  end

  describe "metadata with keyword list syntax" do
    test "set with keyword list and metadata" do
      graph =
        Journey.new_graph("metadata test - keyword list", "v1.0.0", [
          input(:name),
          input(:age)
        ])

      execution = Journey.start_execution(graph)

      execution =
        Journey.set(execution, [name: "Alice", age: 30], metadata: %{"author_id" => "admin"})

      {:ok, %{metadata: name_meta}} = Journey.get(execution, :name)
      {:ok, %{metadata: age_meta}} = Journey.get(execution, :age)

      assert name_meta == %{"author_id" => "admin"}
      assert age_meta == %{"author_id" => "admin"}
    end
  end

  describe "metadata persistence across reloads" do
    test "metadata persists after reloading execution" do
      graph =
        Journey.new_graph("metadata test - persistence", "v1.0.0", [
          input(:title)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :title, "Persistent", metadata: %{"source" => "api"})

      # Reload execution
      reloaded = Journey.load(execution.id)

      {:ok, %{value: value, metadata: metadata}} = Journey.get(reloaded, :title)
      assert value == "Persistent"
      assert metadata == %{"source" => "api"}
    end
  end
end
