defmodule Journey.Node.ArchiveTest do
  use ExUnit.Case, async: true
  import Journey.Node

  describe "archive() |" do
    test "basic validation" do
      graph =
        Journey.new_graph(
          "archive test graph #{__MODULE__}",
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
      assert execution.archived_at == nil
      execution = Journey.set_value(execution, :user_name, "John Doe")
      assert {:ok, "Hello, John Doe"} = Journey.get_value(execution, :greeting, wait: true)
      {:ok, _a} = Journey.get_value(execution, :archival, wait: true)
      execution = execution |> Journey.load()
      assert execution.archived_at != nil
    end
  end
end
