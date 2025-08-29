defmodule Journey.SqlInjectionTest do
  use ExUnit.Case, async: true

  import Journey.Node

  describe "SQL injection protection in value filters" do
    test "malicious string values are safely parameterized" do
      graph =
        Journey.new_graph(
          "sql_injection_test_#{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [input(:test_field), input(:name)]
        )

      # Create some legitimate data
      _exec1 = Journey.start_execution(graph) |> Journey.set_value(:test_field, 42) |> Journey.set_value(:name, "alice")
      _exec2 = Journey.start_execution(graph) |> Journey.set_value(:test_field, 100) |> Journey.set_value(:name, "bob")

      # Test various SQL injection attempts as filter values
      malicious_payloads = [
        # Classic SQL injection attempts
        "'; DROP TABLE executions; --",
        "' OR '1'='1",
        "1; DELETE FROM values; --",
        "42'; INSERT INTO executions (id) VALUES ('hacked'); --",

        # JSONB-specific injection attempts
        "{}'; DROP TABLE values; --",
        "'::text; DELETE FROM executions; --",

        # PostgreSQL function injection attempts
        "pg_sleep(5)",
        "version()",

        # Mixed type confusion attempts
        "42 OR 1=1",
        "true'; DROP TABLE executions; --"
      ]

      # Test each malicious payload - all should be safely handled without SQL injection
      for payload <- malicious_payloads do
        # All string payloads should be safely parameterized (no crashes or injection)
        result = Journey.list_executions(graph_name: graph.name, filter_by: [{:name, :eq, payload}])
        assert is_list(result)
        # Should find no matches for malicious strings
        assert Enum.empty?(result)

        # Numeric field with string payload should also be safely handled
        result2 = Journey.list_executions(graph_name: graph.name, filter_by: [{:test_field, :gt, payload}])
        assert is_list(result2)
        # Should find no matches
        assert Enum.empty?(result2)
      end

      # Test that complex objects are properly rejected
      assert_raise ArgumentError, ~r/Unsupported value type/, fn ->
        Journey.list_executions(graph_name: graph.name, filter_by: [{:name, :eq, %{evil: "payload"}}])
      end
    end

    test "malicious node names are safely handled" do
      graph =
        Journey.new_graph(
          "node_name_injection_test_#{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [input(:legitimate_field)]
        )

      _exec = Journey.start_execution(graph) |> Journey.set_value(:legitimate_field, "test_value")

      # Test malicious node names - these should raise validation errors, not cause SQL injection
      malicious_node_names = [
        :"'; DROP TABLE executions; --",
        :"test_field'; DELETE FROM values WHERE 1=1; --",
        :"field OR 1=1"
      ]

      for malicious_node <- malicious_node_names do
        # Malicious node names should be safely handled - return empty results, no SQL injection
        result = Journey.list_executions(graph_name: graph.name, filter_by: [{malicious_node, :eq, "test"}])
        assert is_list(result)
        # Should find no matches for non-existent nodes
        assert Enum.empty?(result)
      end
    end

    test "malicious graph names are safely parameterized" do
      # Test that malicious graph names don't cause SQL injection
      malicious_graph_names = [
        "'; DROP TABLE executions; SELECT * FROM executions WHERE graph_name = '",
        "test' OR '1'='1' OR graph_name = '",
        "test'; DELETE FROM executions; SELECT * FROM executions WHERE graph_name = '"
      ]

      for malicious_name <- malicious_graph_names do
        # This should safely return empty results, not execute malicious SQL
        result = Journey.list_executions(graph_name: malicious_name)
        assert is_list(result)
        assert Enum.empty?(result)
      end
    end

    test "complex value filters with mixed malicious input" do
      graph =
        Journey.new_graph(
          "complex_injection_test_#{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [input(:num_field), input(:str_field), input(:bool_field)]
        )

      # Create legitimate data
      _exec =
        Journey.start_execution(graph)
        |> Journey.set_value(:num_field, 50)
        |> Journey.set_value(:str_field, "normal_string")
        |> Journey.set_value(:bool_field, true)

      # Test multiple filters with malicious content - should be safely handled
      result =
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [
            # legitimate
            {:num_field, :gt, 30},
            # malicious but safely parameterized
            {:str_field, :eq, "'; DROP TABLE executions; --"},
            # legitimate
            {:bool_field, :eq, false}
          ]
        )

      assert is_list(result)
      # No matches for the malicious string
      assert Enum.empty?(result)

      # Test that unsupported data types are properly rejected
      assert_raise ArgumentError, ~r/Unsupported value type/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [
            {:num_field, :gt, %{malicious: "object"}}
          ]
        )
      end

      # Test that legitimate complex filters work correctly
      result =
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [
            {:num_field, :gte, 40},
            {:str_field, :neq, "different_string"}
          ]
        )

      assert is_list(result)
      assert length(result) == 1
    end

    test "database state remains intact after injection attempts" do
      # Verify that our database state is not corrupted by injection attempts
      graph =
        Journey.new_graph(
          "integrity_test_#{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [input(:test_field)]
        )

      # Create some data
      exec1 = Journey.start_execution(graph) |> Journey.set_value(:test_field, "before_injection")

      # Count records before injection attempts
      initial_executions = Journey.list_executions(graph_name: graph.name)
      initial_count = length(initial_executions)

      # Attempt various injections (should be safely handled)
      injection_attempts = [
        "'; DROP TABLE executions; --",
        "'; DELETE FROM values; --",
        "'; UPDATE executions SET archived_at = 1; --"
      ]

      for injection <- injection_attempts do
        # These should not modify the database
        result = Journey.list_executions(graph_name: graph.name, filter_by: [{:test_field, :eq, injection}])
        assert is_list(result)
      end

      # Create more data after injection attempts
      _exec2 = Journey.start_execution(graph) |> Journey.set_value(:test_field, "after_injection")

      # Verify database integrity - should have original + new data
      final_executions = Journey.list_executions(graph_name: graph.name)
      assert length(final_executions) == initial_count + 1

      # Verify our original data is still intact
      reloaded_exec1 = Journey.load(exec1)
      assert Journey.get_value(reloaded_exec1, :test_field) == {:ok, "before_injection"}
    end

    test "edge cases with special PostgreSQL characters" do
      graph =
        Journey.new_graph(
          "special_chars_test_#{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [input(:text_field)]
        )

      # Create data with special PostgreSQL characters that need proper escaping
      special_values = [
        "text with 'single quotes'",
        "text with \"double quotes\"",
        "text with $$ dollar quotes $$",
        "text with \\ backslashes \\",
        "text with \n newlines \n",
        "text with ; semicolons ;",
        "text with -- comments --"
      ]

      # Create executions with special characters
      for value <- special_values do
        _exec = Journey.start_execution(graph) |> Journey.set_value(:text_field, value)
      end

      # Verify we can filter by these values safely
      for value <- special_values do
        result = Journey.list_executions(graph_name: graph.name, filter_by: [{:text_field, :eq, value}])
        assert length(result) == 1

        found_exec = hd(result)
        assert Journey.get_value(found_exec, :text_field) == {:ok, value}
      end

      # Test string comparisons with special characters
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:text_field, :lt, "text with zzz"}])
      assert length(result) == length(special_values)
    end
  end
end
