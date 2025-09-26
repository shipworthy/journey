defmodule Journey.CustomExecutionPrefixTest do
  use ExUnit.Case
  import Journey.Node

  describe "custom execution ID prefix" do
    test "uses custom prefix when specified" do
      graph = Journey.new_graph("custom prefix test", "v1", [input(:name)], execution_id_prefix: "mygraph")
      execution = Journey.start_execution(graph)

      assert String.starts_with?(execution.id, "MYGRAPH")
      # "MYGRAPH" (7) + 20 random chars
      assert String.length(execution.id) == 27
    end

    test "uses default EXEC prefix when not specified" do
      graph = Journey.new_graph("default prefix test", "v1", [input(:name)])
      execution = Journey.start_execution(graph)

      assert String.starts_with?(execution.id, "EXEC")
      # "EXEC" (4) + 20 random chars
      assert String.length(execution.id) == 24
    end

    test "prefix is normalized to uppercase" do
      graph = Journey.new_graph("lowercase prefix test", "v1", [input(:name)], execution_id_prefix: "lowercase")
      execution = Journey.start_execution(graph)

      assert String.starts_with?(execution.id, "LOWERCASE")
    end

    test "prefix works with mixed case" do
      graph = Journey.new_graph("mixed case prefix test", "v1", [input(:name)], execution_id_prefix: "MiXeD")
      execution = Journey.start_execution(graph)

      assert String.starts_with?(execution.id, "MIXED")
    end

    test "multiple executions with same prefix have different IDs" do
      graph = Journey.new_graph("unique ID test", "v1", [input(:name)], execution_id_prefix: "test")
      execution1 = Journey.start_execution(graph)
      execution2 = Journey.start_execution(graph)

      assert String.starts_with?(execution1.id, "TEST")
      assert String.starts_with?(execution2.id, "TEST")
      assert execution1.id != execution2.id
    end
  end
end
