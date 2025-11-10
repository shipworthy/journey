defmodule Journey.JourneyListExecutionsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "list_executions" do
    test "sunny day, limit / offset" do
      graph = basic_graph(random_string())
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set(:first_name, i)

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 20)
      assert Enum.count(listed_executions) == 20

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 11, offset: 30)
      assert Enum.count(listed_executions) == 11

      listed_executions = Journey.list_executions(graph_name: graph.name, limit: 20, offset: 90)
      assert Enum.count(listed_executions) == 10
    end

    test "sunny day, filer by value" do
      graph = basic_graph(random_string())
      for i <- 1..100, do: Journey.start_execution(graph) |> Journey.set(:first_name, i)

      listed_executions = Journey.list_executions(graph_name: graph.name)
      assert Enum.count(listed_executions) == 100

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :lt, 20}])
      assert Enum.count(some_executions) == 19

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :lte, 20}])
      assert Enum.count(some_executions) == 20

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, 50}])
      assert Enum.count(some_executions) == 1

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :neq, 50}])
      assert Enum.count(some_executions) == 99

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :gt, 60}])
      assert Enum.count(some_executions) == 40

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :gte, 60}])
      assert Enum.count(some_executions) == 41

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :in, [20, 22]}])
      assert Enum.count(some_executions) == 2

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :neq, 1}])
      assert Enum.count(some_executions) == 99

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :is_set}])
      assert Enum.count(some_executions) == 100

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :is_not_set}])
      assert some_executions == []

      some_executions = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, 1}])
      assert Enum.count(some_executions) == 1
    end

    test "sunny day, by graph name" do
      execution =
        basic_graph(random_string())
        |> Journey.start_execution()

      Process.sleep(1_000)
      listed_executions = Journey.list_executions(graph_name: execution.graph_name)

      for le <- listed_executions do
        # Making sure that values and computations are loaded.
        assert Enum.count(le.values) == 4
        assert Enum.count(le.computations) == 1, "#{inspect(le.computations)}"
      end

      assert execution.id in (listed_executions |> Enum.map(& &1.id))
    end

    test "sunny day, sort by inserted_at (which is updated after a set_value)" do
      test_id = random_string()

      execution_ids =
        Enum.map(1..3, fn _ ->
          basic_graph(test_id)
          |> Journey.start_execution()
          |> Map.get(:id)
          |> tap(fn _ -> Process.sleep(1_000) end)
        end)

      # Updating the first execution should put it at the back.
      [first_id | remaining_ids] = execution_ids

      updated_execution =
        first_id
        |> Journey.load()
        |> Journey.set(:first_name, "Mario")

      expected_order = remaining_ids ++ [first_id]
      {:ok, "Hello, Mario", _} = Journey.get(updated_execution, :greeting, wait: :any)

      listed_execution_ids =
        Journey.list_executions(graph_name: basic_graph(test_id).name, order_by_execution_fields: [:updated_at])
        |> Enum.map(fn execution -> execution.id end)
        |> Enum.filter(fn id -> id in execution_ids end)

      assert expected_order == listed_execution_ids
    end

    test "no executions" do
      assert Journey.list_executions(graph_name: "no_such_graph") == []
    end

    test "unexpected option" do
      assert_raise ArgumentError,
                   "Unknown options: [:graph]. Known options: [:filter_by, :graph_name, :graph_version, :include_archived, :limit, :offset, :order_by_execution_fields, :sort_by, :value_filters].",
                   fn ->
                     Journey.list_executions(graph: "no_such_graph")
                   end
    end

    test "filter by graph_version" do
      graph_name = "version_filter_test_#{random_string()}"
      graph_v1 = Journey.new_graph(graph_name, "v1.0.0", [input(:value)])
      graph_v2 = Journey.new_graph(graph_name, "v2.0.0", [input(:value)])

      Journey.start_execution(graph_v1) |> Journey.set(:value, "v1_1")
      Journey.start_execution(graph_v1) |> Journey.set(:value, "v1_2")
      Journey.start_execution(graph_v2) |> Journey.set(:value, "v2_1")

      # Test filtering by version with graph_name
      v1_executions = Journey.list_executions(graph_name: graph_name, graph_version: "v1.0.0")
      assert length(v1_executions) == 2

      v2_executions = Journey.list_executions(graph_name: graph_name, graph_version: "v2.0.0")
      assert length(v2_executions) == 1

      # Test without version filter
      all_executions = Journey.list_executions(graph_name: graph_name)
      assert length(all_executions) == 3
    end

    test "graph_version requires graph_name" do
      assert_raise ArgumentError, "Option :graph_version requires :graph_name to be specified", fn ->
        Journey.list_executions(graph_version: "v1.0.0")
      end
    end

    test "sorting with tuple syntax and mixed formats" do
      test_id = random_string()
      graph = basic_graph(test_id)

      # Create a few executions
      execution_ids =
        Enum.map(1..3, fn i ->
          execution = Journey.start_execution(graph) |> Journey.set(:first_name, "User#{i}")
          # Ensure different timestamps (1 second granularity)
          Process.sleep(1100)
          execution.id
        end)

      # Test that tuple syntax doesn't crash and returns results
      desc_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [inserted_at: :desc]
        )

      desc_ids =
        Enum.map(desc_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert length(desc_ids) == 3

      # Test ascending order also works
      asc_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [inserted_at: :asc]
        )

      asc_ids = Enum.map(asc_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)
      assert length(asc_ids) == 3

      # Verify both queries return the same executions
      assert Enum.sort(asc_ids) == Enum.sort(desc_ids)

      # Test mixed directions - multiple sort fields
      mixed_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [graph_name: :asc, inserted_at: :desc]
        )

      mixed_ids =
        Enum.map(mixed_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert length(mixed_ids) == 3

      # Test that the actual sorting direction is correctly applied
      # Create two executions with a clear time difference in a fresh graph
      specific_graph = basic_graph("#{test_id}_specific")
      _e1 = Journey.start_execution(specific_graph) |> Journey.set(:first_name, "First")
      Process.sleep(1100)
      _e2 = Journey.start_execution(specific_graph) |> Journey.set(:first_name, "Second")

      # Test descending - newer should come first
      desc_specific =
        Journey.list_executions(
          graph_name: specific_graph.name,
          order_by_execution_fields: [inserted_at: :desc]
        )

      # Just verify that both directions work without syntax errors and return expected counts
      asc_specific =
        Journey.list_executions(
          graph_name: specific_graph.name,
          order_by_execution_fields: [inserted_at: :asc]
        )

      assert length(desc_specific) == 2
      assert length(asc_specific) == 2

      # Verify the timestamps are actually different and in the expected order
      desc_timestamps = Enum.map(desc_specific, fn execution -> execution.inserted_at end)
      asc_timestamps = Enum.map(asc_specific, fn execution -> execution.inserted_at end)

      # For descending, larger timestamps should come first
      assert desc_timestamps == Enum.sort(desc_timestamps, :desc)
      # For ascending, smaller timestamps should come first
      assert asc_timestamps == Enum.sort(asc_timestamps, :asc)
    end

    test "atom format functionality" do
      test_id = random_string()
      graph = basic_graph(test_id)

      # Create executions
      execution_ids =
        Enum.map(1..3, fn i ->
          execution = Journey.start_execution(graph) |> Journey.set(:first_name, "User#{i}")
          Process.sleep(1100)
          execution.id
        end)

      # Atom format should work (defaults to ascending)
      legacy_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [:inserted_at]
        )

      legacy_ids =
        Enum.map(legacy_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert legacy_ids == execution_ids
    end

    test "mixed format: atoms and tuples together" do
      test_id = random_string()
      graph = basic_graph(test_id)

      # Create executions
      execution_ids =
        Enum.map(1..3, fn i ->
          execution = Journey.start_execution(graph) |> Journey.set(:first_name, "User#{i}")
          Process.sleep(1100)
          execution.id
        end)

      # Mixed format: combine atom and tuple syntax
      mixed_results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [:graph_name, :revision, inserted_at: :desc]
        )

      mixed_ids =
        Enum.map(mixed_results, fn execution -> execution.id end) |> Enum.filter(fn id -> id in execution_ids end)

      assert length(mixed_ids) == 3
    end

    test "invalid sort field format raises error" do
      graph = basic_graph(random_string())

      # Invalid direction
      assert_raise ArgumentError, ~r/Invalid sort field format.*Expected atom or/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [inserted_at: :invalid]
        )
      end

      # Invalid tuple format
      assert_raise ArgumentError, ~r/Invalid sort field format.*Expected atom or/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [{"invalid", :asc}]
        )
      end

      # Invalid entry type
      assert_raise ArgumentError, ~r/Invalid sort field format.*Expected atom or/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: ["invalid_string"]
        )
      end
    end

    test "mixed data types: string and integer filtering" do
      graph = basic_graph(random_string())

      # Create executions with string names
      exec_alice = Journey.start_execution(graph) |> Journey.set(:first_name, "alice")
      exec_bob = Journey.start_execution(graph) |> Journey.set(:first_name, "bob")
      _exec_charlie = Journey.start_execution(graph) |> Journey.set(:first_name, "charlie")

      # Create executions with integer names
      _exec_100 = Journey.start_execution(graph) |> Journey.set(:first_name, 100)
      exec_200 = Journey.start_execution(graph) |> Journey.set(:first_name, 200)
      exec_300 = Journey.start_execution(graph) |> Journey.set(:first_name, 300)

      # String filtering should only match string values
      string_eq_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, "alice"}])
      assert length(string_eq_results) == 1
      assert hd(string_eq_results).id == exec_alice.id

      string_lt_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :lt, "charlie"}])

      # "alice" and "bob" are < "charlie"
      assert length(string_lt_results) == 2
      string_ids = Enum.map(string_lt_results, & &1.id) |> Enum.sort()
      expected_string_ids = [exec_alice.id, exec_bob.id] |> Enum.sort()
      assert string_ids == expected_string_ids

      # Integer filtering should only match numeric values
      int_eq_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, 200}])
      assert length(int_eq_results) == 1
      assert hd(int_eq_results).id == exec_200.id

      int_gt_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :gt, 150}])
      # 200 and 300 are > 150
      assert length(int_gt_results) == 2
      int_ids = Enum.map(int_gt_results, & &1.id) |> Enum.sort()
      expected_int_ids = [exec_200.id, exec_300.id] |> Enum.sort()
      assert int_ids == expected_int_ids

      # Cross-type queries return empty (no errors)
      # Query for numeric value against string data - should find nothing
      cross_type_numeric = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, 999}])
      assert Enum.empty?(cross_type_numeric)

      # Query for string value against numeric data - should find nothing
      cross_type_string = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :eq, "999"}])
      assert Enum.empty?(cross_type_string)

      # Verify total count is still correct
      all_results = Journey.list_executions(graph_name: graph.name)
      # 3 string + 3 integer executions
      assert length(all_results) == 6
    end

    test "contains operator for substring matching" do
      graph = basic_graph(random_string())

      # Create executions with various email addresses
      exec_gmail_alice = Journey.start_execution(graph) |> Journey.set(:first_name, "alice@gmail.com")
      exec_gmail_bob = Journey.start_execution(graph) |> Journey.set(:first_name, "bob@gmail.com")
      exec_yahoo_charlie = Journey.start_execution(graph) |> Journey.set(:first_name, "charlie@yahoo.com")
      _exec_company_dave = Journey.start_execution(graph) |> Journey.set(:first_name, "dave@company.org")
      _exec_integer = Journey.start_execution(graph) |> Journey.set(:first_name, 12_345)

      # Test basic substring matching
      gmail_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "@gmail"}])
      assert length(gmail_results) == 2
      gmail_ids = Enum.map(gmail_results, & &1.id) |> Enum.sort()
      expected_gmail_ids = [exec_gmail_alice.id, exec_gmail_bob.id] |> Enum.sort()
      assert gmail_ids == expected_gmail_ids

      # Test matching at beginning of string
      alice_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "alice"}])
      assert length(alice_results) == 1
      assert hd(alice_results).id == exec_gmail_alice.id

      # Test matching at end of string
      com_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, ".com"}])
      assert length(com_results) == 3
      com_ids = Enum.map(com_results, & &1.id) |> Enum.sort()
      expected_com_ids = [exec_gmail_alice.id, exec_gmail_bob.id, exec_yahoo_charlie.id] |> Enum.sort()
      assert com_ids == expected_com_ids

      # Test no matches
      no_match_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "notfound"}])

      assert Enum.empty?(no_match_results)

      # Test case sensitivity
      case_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "ALICE"}])
      assert Enum.empty?(case_results)

      # Test that non-string values are ignored (integer value should not match)
      numeric_contains_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "123"}])

      assert Enum.empty?(numeric_contains_results)

      # Test empty string pattern (should match all string values)
      empty_pattern_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, ""}])
      # All string values, not the integer
      assert length(empty_pattern_results) == 4

      # Test single character matching
      at_symbol_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "@"}])
      # All email addresses
      assert length(at_symbol_results) == 4
    end

    test "icontains operator for case-insensitive substring matching" do
      graph = basic_graph(random_string())

      # Create executions with various email addresses in different cases
      exec_gmail_alice = Journey.start_execution(graph) |> Journey.set(:first_name, "Alice@Gmail.com")
      exec_gmail_bob = Journey.start_execution(graph) |> Journey.set(:first_name, "BOB@gmail.COM")
      exec_yahoo_charlie = Journey.start_execution(graph) |> Journey.set(:first_name, "charlie@YAHOO.com")
      exec_company_dave = Journey.start_execution(graph) |> Journey.set(:first_name, "Dave@Company.ORG")
      _exec_integer = Journey.start_execution(graph) |> Journey.set(:first_name, 12_345)

      # Test case-insensitive substring matching with lowercase pattern
      gmail_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "@gmail"}])
      assert length(gmail_results) == 2
      gmail_ids = Enum.map(gmail_results, & &1.id) |> Enum.sort()
      expected_gmail_ids = [exec_gmail_alice.id, exec_gmail_bob.id] |> Enum.sort()
      assert gmail_ids == expected_gmail_ids

      # Test case-insensitive matching with uppercase pattern
      alice_upper_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "ALICE"}])

      assert length(alice_upper_results) == 1
      assert hd(alice_upper_results).id == exec_gmail_alice.id

      # Test case-insensitive matching with mixed case pattern
      yahoo_mixed_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "YaHoO"}])

      assert length(yahoo_mixed_results) == 1
      assert hd(yahoo_mixed_results).id == exec_yahoo_charlie.id

      # Test case-insensitive matching at end of string
      com_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, ".COM"}])
      assert length(com_results) == 3
      com_ids = Enum.map(com_results, & &1.id) |> Enum.sort()
      expected_com_ids = [exec_gmail_alice.id, exec_gmail_bob.id, exec_yahoo_charlie.id] |> Enum.sort()
      assert com_ids == expected_com_ids

      # Test that non-string values are ignored (integer value should not match)
      numeric_icontains_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "123"}])

      assert Enum.empty?(numeric_icontains_results)

      # Test empty string pattern (should match all string values)
      empty_pattern_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, ""}])

      # All string values, not the integer
      assert length(empty_pattern_results) == 4

      # Test case-insensitive single character matching
      at_symbol_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "@"}])
      # All email addresses
      assert length(at_symbol_results) == 4

      # Compare with case-sensitive :contains to show the difference
      case_sensitive_alice =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "alice"}])

      # Should only match lowercase "charlie" (not uppercase "Alice")
      assert Enum.empty?(case_sensitive_alice)

      case_insensitive_alice =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "alice"}])

      # Should match "Alice" regardless of case
      assert length(case_insensitive_alice) == 1
      assert hd(case_insensitive_alice).id == exec_gmail_alice.id

      # Test with .org domain (case insensitive)
      org_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, ".org"}])
      assert length(org_results) == 1
      assert hd(org_results).id == exec_company_dave.id
    end

    test "literal wildcard character handling in :contains and :icontains" do
      graph = basic_graph(random_string())

      # Create executions with literal wildcard characters
      exec_percent = Journey.start_execution(graph) |> Journey.set(:first_name, "10% discount")
      exec_underscore = Journey.start_execution(graph) |> Journey.set(:first_name, "user_name")
      exec_backslash = Journey.start_execution(graph) |> Journey.set(:first_name, "path\\file")
      exec_combined = Journey.start_execution(graph) |> Journey.set(:first_name, "50%_off\\today")
      exec_normal = Journey.start_execution(graph) |> Journey.set(:first_name, "normal_text")

      # Test searching for literal % (should not match everything)
      percent_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "%"}])
      assert length(percent_results) == 2
      percent_ids = Enum.map(percent_results, & &1.id) |> Enum.sort()
      expected_percent_ids = [exec_percent.id, exec_combined.id] |> Enum.sort()
      assert percent_ids == expected_percent_ids

      # Test searching for literal _ (should not match single characters)
      underscore_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "_"}])
      assert length(underscore_results) == 3
      underscore_ids = Enum.map(underscore_results, & &1.id) |> Enum.sort()
      expected_underscore_ids = [exec_underscore.id, exec_combined.id, exec_normal.id] |> Enum.sort()
      assert underscore_ids == expected_underscore_ids

      # Test searching for literal \ (should not escape the following character)
      backslash_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "\\"}])
      assert length(backslash_results) == 2
      backslash_ids = Enum.map(backslash_results, & &1.id) |> Enum.sort()
      expected_backslash_ids = [exec_backslash.id, exec_combined.id] |> Enum.sort()
      assert backslash_ids == expected_backslash_ids

      # Test case-insensitive literal wildcard matching
      icontains_percent = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "%"}])
      assert length(icontains_percent) == 2

      # Test that patterns still work correctly for substring matching
      discount_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "discount"}])

      assert length(discount_results) == 1
      assert hd(discount_results).id == exec_percent.id

      # Test complex pattern with multiple wildcards
      complex_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "%_"}])
      assert length(complex_results) == 1
      assert hd(complex_results).id == exec_combined.id
    end

    test "list_contains operator for list element matching" do
      graph = list_graph(random_string())

      # Create executions with different list values
      exec_string_list = Journey.start_execution(graph) |> Journey.set(:recipients, ["user1", "user2", "admin"])
      exec_mixed_list = Journey.start_execution(graph) |> Journey.set(:recipients, ["user3", 42, "user4"])
      exec_integer_list = Journey.start_execution(graph) |> Journey.set(:recipients, [1, 2, 3, 4])
      exec_empty_list = Journey.start_execution(graph) |> Journey.set(:recipients, [])
      _exec_non_list = Journey.start_execution(graph) |> Journey.set(:recipients, "not_a_list")
      exec_nil_value = Journey.start_execution(graph)

      # Test finding string element in list
      user1_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "user1"}])

      assert length(user1_results) == 1
      assert hd(user1_results).id == exec_string_list.id

      # Test finding string element that appears in mixed type list
      user3_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "user3"}])

      assert length(user3_results) == 1
      assert hd(user3_results).id == exec_mixed_list.id

      # Test finding integer element in list
      integer_2_results = Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, 2}])
      assert length(integer_2_results) == 1
      assert hd(integer_2_results).id == exec_integer_list.id

      # Test finding integer element in mixed list
      integer_42_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, 42}])

      assert length(integer_42_results) == 1
      assert hd(integer_42_results).id == exec_mixed_list.id

      # Test cross-type matching: string "42" should NOT match integer 42 (JSON is type-strict)
      string_42_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "42"}])

      assert Enum.empty?(string_42_results)

      # Test no matches for non-existent element
      no_match_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "not_found"}])

      assert Enum.empty?(no_match_results)

      # Test empty list returns no results
      empty_list_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "anything"}])

      refute Enum.any?(empty_list_results, fn exec -> exec.id == exec_empty_list.id end)

      # Test non-list values return no results
      non_list_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "not_a_list"}])

      assert Enum.empty?(non_list_results)

      # Test nil values return no results
      nil_results =
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "anything"}])

      refute Enum.any?(nil_results, fn exec -> exec.id == exec_nil_value.id end)

      # Test multiple list_contains filters (AND logic)
      combined_results =
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [
            {:recipients, :list_contains, "user3"},
            {:recipients, :list_contains, 42}
          ]
        )

      assert length(combined_results) == 1
      assert hd(combined_results).id == exec_mixed_list.id
    end

    test "list_contains validation errors" do
      graph = list_graph(random_string())

      # Test invalid value type - boolean
      assert_raise ArgumentError, fn ->
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, true}])
      end

      # Test invalid value type - nil
      assert_raise ArgumentError, fn ->
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, nil}])
      end

      # Test invalid value type - map
      assert_raise ArgumentError, fn ->
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, %{key: "value"}}])
      end

      # Test invalid value type - list
      assert_raise ArgumentError, fn ->
        Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, ["nested", "list"]}])
      end
    end

    test "sort_by with value fields" do
      graph = basic_graph(random_string())

      # Create executions with different first_name values
      exec_3 = Journey.start_execution(graph) |> Journey.set(:first_name, 3)
      exec_1 = Journey.start_execution(graph) |> Journey.set(:first_name, 1)
      exec_2 = Journey.start_execution(graph) |> Journey.set(:first_name, 2)

      # Sort by value field ascending
      asc_results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [:first_name]
        )

      asc_ids = Enum.map(asc_results, & &1.id)
      assert asc_ids == [exec_1.id, exec_2.id, exec_3.id]

      # Sort by value field descending
      desc_results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [{:first_name, :desc}]
        )

      desc_ids = Enum.map(desc_results, & &1.id)
      assert desc_ids == [exec_3.id, exec_2.id, exec_1.id]
    end

    test "sort_by mixing execution and value fields" do
      test_id = random_string()
      graph = basic_graph(test_id)

      # Create executions with different values
      exec_1 = Journey.start_execution(graph) |> Journey.set(:first_name, "alice")
      Process.sleep(1100)
      exec_2 = Journey.start_execution(graph) |> Journey.set(:first_name, "bob")
      Process.sleep(1100)
      exec_3 = Journey.start_execution(graph) |> Journey.set(:first_name, "alice")

      # Sort by value field first, then by execution field
      results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [:first_name, :inserted_at]
        )

      ids = Enum.map(results, & &1.id)
      # Should be sorted by first_name first (alice, alice, bob), then by inserted_at
      assert ids == [exec_1.id, exec_3.id, exec_2.id]

      # Sort by value field descending, then execution field
      results_desc =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [{:first_name, :desc}, :inserted_at]
        )

      ids_desc = Enum.map(results_desc, & &1.id)
      # Should be sorted by first_name desc (bob, alice, alice), then by inserted_at
      assert ids_desc == [exec_2.id, exec_1.id, exec_3.id]
    end

    test "sort_by with NULL values" do
      graph = basic_graph(random_string())

      # Create executions - some with values, some without
      # No first_name set
      _exec_nil_1 = Journey.start_execution(graph)
      _exec_2 = Journey.start_execution(graph) |> Journey.set(:first_name, 2)
      # No first_name set
      _exec_nil_2 = Journey.start_execution(graph)
      _exec_1 = Journey.start_execution(graph) |> Journey.set(:first_name, 1)

      # Sort ascending - check the order is correct
      asc_results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [:first_name]
        )

      # Get the actual first_name values in the sorted order
      asc_values =
        asc_results
        |> Enum.map(fn execution ->
          case Enum.find(execution.values, fn v -> v.node_name == :first_name end) do
            nil -> :null
            %{set_time: nil} -> :null
            %{node_value: value} -> value
          end
        end)

      # For JSONB NULL values, PostgreSQL puts NULLs at the end for ASC
      expected_asc_order = [1, 2, :null, :null]
      assert asc_values == expected_asc_order

      # Sort descending - check the order is correct
      desc_results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [first_name: :desc]
        )

      desc_values =
        desc_results
        |> Enum.map(fn execution ->
          case Enum.find(execution.values, fn v -> v.node_name == :first_name end) do
            nil -> :null
            %{set_time: nil} -> :null
            %{node_value: value} -> value
          end
        end)

      # For JSONB NULL values with DESC, PostgreSQL puts NULLs at the beginning
      expected_desc_order = [:null, :null, 2, 1]
      assert desc_values == expected_desc_order
    end

    test "sort_by validates non-existent value fields" do
      graph = basic_graph(random_string())
      Journey.start_execution(graph)

      # Should raise error for non-existent field
      assert_raise ArgumentError, ~r/Sort field :nonexistent does not exist/, fn ->
        Journey.list_executions(
          graph_name: graph.name,
          graph_version: "1.0.0",
          sort_by: [:nonexistent]
        )
      end
    end

    test "order_by_execution_fields still works as alias" do
      graph = basic_graph(random_string())

      exec_1 = Journey.start_execution(graph) |> Journey.set(:first_name, "first")
      Process.sleep(1100)
      exec_2 = Journey.start_execution(graph) |> Journey.set(:first_name, "second")

      # Old parameter should still work
      results =
        Journey.list_executions(
          graph_name: graph.name,
          order_by_execution_fields: [:inserted_at]
        )

      ids = Enum.map(results, & &1.id)
      assert ids == [exec_1.id, exec_2.id]
    end

    test "sort_by takes precedence over order_by_execution_fields" do
      graph = basic_graph(random_string())

      exec_1 = Journey.start_execution(graph) |> Journey.set(:first_name, 1)
      exec_2 = Journey.start_execution(graph) |> Journey.set(:first_name, 2)

      # When both are provided, sort_by should win
      results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [{:first_name, :desc}],
          # This should be ignored
          order_by_execution_fields: [:first_name]
        )

      ids = Enum.map(results, & &1.id)
      # Descending order from sort_by
      assert ids == [exec_2.id, exec_1.id]
    end

    test "sort_by with multiple value fields" do
      # Create a dedicated graph with two input nodes for this test
      test_id = random_string()
      graph_name = "multi_value_sort_test_#{test_id}"

      graph =
        Journey.new_graph(
          graph_name,
          "1.0.0",
          [
            input(:priority),
            input(:first_name)
          ]
        )

      # Create test data with combinations that clearly show precedence
      # Priority "high" + names "bob", "alice" (different insertion order to test sorting)
      exec_high_bob =
        Journey.start_execution(graph) |> Journey.set(:priority, "high") |> Journey.set(:first_name, "bob")

      exec_high_alice =
        Journey.start_execution(graph)
        |> Journey.set(:priority, "high")
        |> Journey.set(:first_name, "alice")

      # Priority "low" + names "charlie", "alice"
      exec_low_charlie =
        Journey.start_execution(graph)
        |> Journey.set(:priority, "low")
        |> Journey.set(:first_name, "charlie")

      exec_low_alice =
        Journey.start_execution(graph) |> Journey.set(:priority, "low") |> Journey.set(:first_name, "alice")

      # Test ascending sort: [:priority, :first_name]
      # Should group by priority first ("high", then "low"), then by first_name within each group
      asc_results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [:priority, :first_name]
        )

      asc_ids = Enum.map(asc_results, & &1.id)
      # Expected: high priority group (alice, bob), then low priority group (alice, charlie)
      assert asc_ids == [exec_high_alice.id, exec_high_bob.id, exec_low_alice.id, exec_low_charlie.id]

      # Test mixed directions: [priority: :desc, first_name: :asc]
      # Should put "low" priority first (desc), then "high", with names ascending within each group
      mixed_results =
        Journey.list_executions(
          graph_name: graph.name,
          sort_by: [priority: :desc, first_name: :asc]
        )

      mixed_ids = Enum.map(mixed_results, & &1.id)
      # Expected: low priority group (alice, charlie), then high priority group (alice, bob)
      assert mixed_ids == [exec_low_alice.id, exec_low_charlie.id, exec_high_alice.id, exec_high_bob.id]
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "basic graph, greetings #{__MODULE__} #{test_id}",
      "1.0.0",
      [
        input(:first_name),
        compute(
          :greeting,
          unblocked_when({:first_name, &provided?/1}),
          fn %{first_name: first_name} ->
            {:ok, "Hello, #{first_name}"}
          end
        )
      ]
    )
  end

  defp list_graph(test_id) do
    Journey.new_graph(
      "list graph, recipients #{__MODULE__} #{test_id}",
      "1.0.0",
      [input(:recipients)]
    )
  end
end
