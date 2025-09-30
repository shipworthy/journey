defmodule Journey.MetadataTest do
  use ExUnit.Case, async: true
  import Journey.Node

  describe "basic metadata functionality" do
    test "set and get value with metadata" do
      graph =
        Journey.new_graph("metadata test - basic", "v1.0.0", [
          input(:title)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :title, "Hello", metadata: %{author_id: "user123"})

      {:ok, {value, metadata, _revision}} = Journey.get(execution, :title)
      assert value == "Hello"
      assert metadata == %{"author_id" => "user123"}
    end

    test "set without metadata returns nil metadata" do
      graph = Journey.new_graph("metadata test - no metadata", "v1.0.0", [input(:title)])
      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :title, "Hello")

      {:ok, {_value, metadata, _revision}} = Journey.get(execution, :title)
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
          metadata: %{author_id: "user123"}
        )

      {:ok, {_, title_meta, _}} = Journey.get(execution, :title)
      {:ok, {_, desc_meta, _}} = Journey.get(execution, :description)

      assert title_meta == %{"author_id" => "user123"}
      assert desc_meta == %{"author_id" => "user123"}
    end
  end

  describe "f_compute/2 receives metadata" do
    test "f_compute/2 receives metadata from upstream nodes" do
      graph =
        Journey.new_graph("metadata test - compute with metadata", "v1.0.0", [
          input(:title),
          compute(
            :title_with_author,
            [:title],
            fn %{title: title}, metadata_map ->
              author = get_in(metadata_map, [:title, "author_id"]) || "unknown"
              {:ok, "#{title} by #{author}"}
            end
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :title, "Hello", metadata: %{author_id: "user123"})

      {:ok, {result, _, _}} = Journey.get(execution, :title_with_author, wait: :any)
      assert result == "Hello by user123"
    end

    test "f_compute/1 still works without metadata (backward compatibility)" do
      graph =
        Journey.new_graph("metadata test - compute without metadata", "v1.0.0", [
          input(:name),
          compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello #{name}"} end)
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :name, "Alice", metadata: %{author_id: "user123"})

      {:ok, {result, _, _}} = Journey.get(execution, :greeting, wait: :any)
      assert result == "Hello Alice"
    end

    test "f_compute/2 with multiple upstream nodes receives all metadata" do
      graph =
        Journey.new_graph("metadata test - multiple upstream", "v1.0.0", [
          input(:first_name),
          input(:last_name),
          compute(
            :full_name_with_authors,
            [:first_name, :last_name],
            fn %{first_name: first, last_name: last}, metadata_map ->
              first_author = get_in(metadata_map, [:first_name, "author_id"]) || "unknown"
              last_author = get_in(metadata_map, [:last_name, "author_id"]) || "unknown"
              {:ok, "#{first} #{last} (first by #{first_author}, last by #{last_author})"}
            end
          )
        ])

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :first_name, "John", metadata: %{author_id: "user1"})
      execution = Journey.set(execution, :last_name, "Doe", metadata: %{author_id: "user2"})

      {:ok, {result, _, _}} = Journey.get(execution, :full_name_with_authors, wait: :any)
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
      execution = Journey.set(execution, :content, "v1", metadata: %{author_id: "user123"})
      {:ok, {history1, _, _}} = Journey.get(execution, :content_history, wait: :any)

      assert length(history1) == 1
      assert hd(history1)["value"] == "v1"
      assert hd(history1)["metadata"] == %{"author_id" => "user123"}

      # Second change
      execution = Journey.set(execution, :content, "v2", metadata: %{author_id: "user456"})
      {:ok, {history2, _, _}} = Journey.get(execution, :content_history, wait: :newer)

      assert length(history2) == 2
      assert Enum.at(history2, 1)["value"] == "v2"
      assert Enum.at(history2, 1)["metadata"] == %{"author_id" => "user456"}
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
      {:ok, {history, _, _}} = Journey.get(execution, :content_history, wait: :any)

      assert length(history) == 1
      assert hd(history)["value"] == "v1"
      assert hd(history)["metadata"] == nil
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
        Journey.set(execution, [name: "Alice", age: 30], metadata: %{author_id: "admin"})

      {:ok, {_, name_meta, _}} = Journey.get(execution, :name)
      {:ok, {_, age_meta, _}} = Journey.get(execution, :age)

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
      execution = Journey.set(execution, :title, "Persistent", metadata: %{source: "api"})

      # Reload execution
      reloaded = Journey.load(execution.id)

      {:ok, {value, metadata, _revision}} = Journey.get(reloaded, :title)
      assert value == "Persistent"
      assert metadata == %{"source" => "api"}
    end
  end
end
