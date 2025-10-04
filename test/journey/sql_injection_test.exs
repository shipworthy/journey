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
      _exec1 = Journey.start_execution(graph) |> Journey.set(:test_field, 42) |> Journey.set(:name, "alice")
      _exec2 = Journey.start_execution(graph) |> Journey.set(:test_field, 100) |> Journey.set(:name, "bob")

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

      _exec = Journey.start_execution(graph) |> Journey.set(:legitimate_field, "test_value")

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
        |> Journey.set(:num_field, 50)
        |> Journey.set(:str_field, "normal_string")
        |> Journey.set(:bool_field, true)

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
      exec1 = Journey.start_execution(graph) |> Journey.set(:test_field, "before_injection")

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
      _exec2 = Journey.start_execution(graph) |> Journey.set(:test_field, "after_injection")

      # Verify database integrity - should have original + new data
      final_executions = Journey.list_executions(graph_name: graph.name)
      assert length(final_executions) == initial_count + 1

      # Verify our original data is still intact
      reloaded_exec1 = Journey.load(exec1)
      assert {:ok, "before_injection"} = Journey.get_value(reloaded_exec1, :test_field)
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
        _exec = Journey.start_execution(graph) |> Journey.set(:text_field, value)
      end

      # Verify we can filter by these values safely
      for value <- special_values do
        result = Journey.list_executions(graph_name: graph.name, filter_by: [{:text_field, :eq, value}])
        assert length(result) == 1

        found_exec = hd(result)
        assert {:ok, ^value} = Journey.get_value(found_exec, :text_field)
      end

      # Test string comparisons with special characters
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:text_field, :lt, "text with zzz"}])
      assert length(result) == length(special_values)
    end

    test "SQL injection protection for :contains and :icontains operators" do
      graph =
        Journey.new_graph(
          "contains_injection_test_#{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [input(:search_field)]
        )

      # Create legitimate data
      _exec1 = Journey.start_execution(graph) |> Journey.set(:search_field, "user@example.com")
      _exec2 = Journey.start_execution(graph) |> Journey.set(:search_field, "admin@test.org")
      _exec3 = Journey.start_execution(graph) |> Journey.set(:search_field, 12_345)

      # Test SQL injection attempts with :contains operator
      malicious_contains_payloads = [
        # Classic SQL injection attempts
        "'; DROP TABLE executions; --",
        "' OR '1'='1",
        "'; DELETE FROM values; --",

        # LIKE-specific injection attempts
        "' OR (? #>> '{}') LIKE '%' OR '1'='1",
        "%'; DROP TABLE executions; --",
        "_'; INSERT INTO executions VALUES ('hacked'); --",

        # PostgreSQL function injection attempts
        "pg_sleep(5)",
        "version()",

        # JSONB-specific injection attempts
        "{}'; DROP TABLE values; --",
        "'::text; DELETE FROM executions; --"
      ]

      # Test each malicious payload with :contains - all should be safely handled
      for payload <- malicious_contains_payloads do
        result = Journey.list_executions(graph_name: graph.name, filter_by: [{:search_field, :contains, payload}])
        assert is_list(result)
        # Should find no matches for malicious strings
        assert Enum.empty?(result)

        # Also test :icontains
        result2 = Journey.list_executions(graph_name: graph.name, filter_by: [{:search_field, :icontains, payload}])
        assert is_list(result2)
        assert Enum.empty?(result2)
      end

      # Test that legitimate substring searches work correctly
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:search_field, :contains, "@"}])
      # Both email addresses
      assert length(result) == 2

      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:search_field, :icontains, "EXAMPLE"}])
      # Case-insensitive match for user@example.com
      assert length(result) == 1
    end

    test "LIKE wildcard handling for :contains and :icontains operators" do
      graph =
        Journey.new_graph(
          "wildcard_test_#{Journey.Helpers.Random.random_string()}",
          "1.0.0",
          [input(:content)]
        )

      # Create data with LIKE wildcard characters that should be treated as literals
      wildcard_values = [
        "10% increase",
        "user_name_field",
        "path\\to\\file",
        "100% complete",
        "test_case_1",
        "folder\\subfolder",
        "50%_discount",
        "file_name\\path"
      ]

      # Create executions with wildcard characters
      for value <- wildcard_values do
        _exec = Journey.start_execution(graph) |> Journey.set(:content, value)
      end

      # Test that we can search for literal % characters (should not act as wildcard)
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:content, :contains, "%"}])
      expected_percent_matches = ["10% increase", "100% complete", "50%_discount"]
      assert length(result) == 3

      result_values =
        result
        |> Enum.map(fn exec ->
          {:ok, val} = Journey.get_value(exec, :content)
          val
        end)
        |> Enum.sort()

      assert result_values == Enum.sort(expected_percent_matches)

      # Test that we can search for literal _ characters (should not act as wildcard)
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:content, :contains, "_"}])
      expected_underscore_matches = ["user_name_field", "test_case_1", "50%_discount", "file_name\\path"]
      assert length(result) == 4

      result_values =
        result
        |> Enum.map(fn exec ->
          {:ok, val} = Journey.get_value(exec, :content)
          val
        end)
        |> Enum.sort()

      assert result_values == Enum.sort(expected_underscore_matches)

      # Test that we can search for literal \ characters
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:content, :contains, "\\"}])
      expected_backslash_matches = ["path\\to\\file", "folder\\subfolder", "file_name\\path"]
      assert length(result) == 3

      result_values =
        result
        |> Enum.map(fn exec ->
          {:ok, val} = Journey.get_value(exec, :content)
          val
        end)
        |> Enum.sort()

      assert result_values == Enum.sort(expected_backslash_matches)

      # Test case-insensitive matching with :icontains
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:content, :icontains, "USER_"}])
      assert length(result) == 1
      assert {:ok, "user_name_field"} = Journey.get_value(hd(result), :content)

      # Test combination of wildcards
      result = Journey.list_executions(graph_name: graph.name, filter_by: [{:content, :contains, "%_"}])
      assert length(result) == 1
      assert {:ok, "50%_discount"} = Journey.get_value(hd(result), :content)
    end
  end
end
