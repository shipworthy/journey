defmodule Journey.Node.ArchiveTest do
  use ExUnit.Case, async: true
  import Journey.Node

  import Journey.Helpers.Random, only: [random_string: 0]

  describe "archive() |" do
    test "basic validation" do
      graph_name = "archive test graph #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:user_name),
            compute(
              :greeting,
              [:user_name],
              fn %{user_name: user_name} ->
                {:ok, "Hello, #{user_name}"}
              end
            ),
            archive(:archival, [:greeting])
          ]
        )

      execution = Journey.start_execution(graph)

      assert execution.id in (Journey.list_executions(graph_name: graph_name) |> Enum.map(& &1.id))

      assert execution.archived_at == nil
      execution = Journey.set(execution, :user_name, "John Doe")
      {:ok, "Hello, John Doe", _} = Journey.get(execution, :greeting, wait: :any)
      {:ok, _, _} = Journey.get(execution, :archival, wait: :any)

      # The execution is now archived, and it is no longer visible by default.
      assert nil == execution |> Journey.load(), "archived executions are not load'able by default"

      execution = execution |> Journey.load(include_archived: true)
      assert execution.archived_at != nil

      assert execution.id not in (Journey.list_executions(graph_name: graph_name) |> Enum.map(& &1.id))

      assert execution.id in (Journey.list_executions(graph_name: graph_name, include_archived: true)
                              |> Enum.map(& &1.id))
    end
  end

  describe "non-existent execution ID" do
    test "archive/1 raises ArgumentError" do
      assert_raise ArgumentError, ~r/execution not found/, fn ->
        Journey.archive("EXEC_NONEXISTENT")
      end
    end

    test "unarchive/1 raises ArgumentError" do
      assert_raise ArgumentError, ~r/execution not found/, fn ->
        Journey.unarchive("EXEC_NONEXISTENT")
      end
    end
  end

  describe "archive node type |" do
    test "archive nodes have type :archive" do
      graph_name = "archive type test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(graph_name, "1.0.0", [
          input(:x),
          archive(:done, [:x])
        ])

      archive_node = Enum.find(graph.nodes, fn n -> n.name == :done end)
      assert archive_node.type == :archive
    end

    test "archive value survives invalidation when upstream is unset" do
      graph_name = "archive invalidation test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(graph_name, "1.0.0", [
          input(:x),
          archive(:a, [:x])
        ])

      execution = graph |> Journey.start_execution() |> Journey.set(:x, "value")
      {:ok, archived_timestamp, _rev} = Journey.get(execution, :a, wait: :any)
      assert is_integer(archived_timestamp)

      execution = Journey.load(execution, include_archived: true)
      assert execution.archived_at != nil

      :ok = Journey.unarchive(execution)
      execution = Journey.load(execution.id)
      assert execution.archived_at == nil

      # Unsetting :x triggers invalidation. Archive value must survive
      # because archive nodes are intentionally excluded from the clear path.
      execution = Journey.unset(execution, :x)

      execution = Journey.load(execution.id)
      {:ok, ^archived_timestamp, _} = Journey.get(execution, :a)
    end

    test "archive node appears as :archive in introspection output" do
      graph_name = "archive introspection test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(graph_name, "1.0.0", [
          input(:x),
          archive(:a, [:x])
        ])

      execution = graph |> Journey.start_execution() |> Journey.set(:x, "value")
      {:ok, _, _} = Journey.get(execution, :a, wait: :any)

      text = Journey.Tools.introspect(execution.id)
      assert text =~ ~r/:a.*:archive/
      refute text =~ ~r/:a.*:compute/
    end

    test "archive node renders with archive label in mermaid output" do
      graph_name = "archive mermaid test #{__MODULE__}-#{random_string()}"

      graph =
        Journey.new_graph(graph_name, "1.0.0", [
          input(:x),
          archive(:a, [:x])
        ])

      static = Journey.Tools.generate_mermaid_graph(graph)
      assert static =~ "archive node"

      execution = Journey.start_execution(graph)
      live = Journey.Tools.generate_mermaid_execution(execution.id)
      assert live =~ "archive node"
    end
  end
end
