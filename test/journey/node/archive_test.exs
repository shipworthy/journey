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
      execution = Journey.set_value(execution, :user_name, "John Doe")
      assert {:ok, "Hello, John Doe"} = Journey.get_value(execution, :greeting, wait_any: true)
      {:ok, _a} = Journey.get_value(execution, :archival, wait_any: true)

      # The execution is now archived, and it is no longer visible by default.
      assert nil == execution |> Journey.load(), "archived executions are not load'able by default"

      execution = execution |> Journey.load(include_archived: true)
      assert execution.archived_at != nil

      assert execution.id not in (Journey.list_executions(graph_name: graph_name) |> Enum.map(& &1.id))

      assert execution.id in (Journey.list_executions(graph_name: graph_name, include_archived: true)
                              |> Enum.map(& &1.id))
    end
  end
end
