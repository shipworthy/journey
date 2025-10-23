defmodule Journey.JourneyListExecutionsOrAndConditionsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  describe "OR operator" do
    test "simple OR with two conditions" do
      graph = basic_graph(random_string())

      # Create executions with different combinations
      exec_age_only = Journey.start_execution(graph) |> Journey.set(:age, 25) |> Journey.set(:status, "pending")
      exec_status_only = Journey.start_execution(graph) |> Journey.set(:age, 15) |> Journey.set(:status, "verified")
      exec_both = Journey.start_execution(graph) |> Journey.set(:age, 30) |> Journey.set(:status, "verified")
      exec_neither = Journey.start_execution(graph) |> Journey.set(:age, 10) |> Journey.set(:status, "pending")

      # Filter with OR: age > 18 OR status = "verified"
      results =
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [{:or, [{:age, :gt, 18}, {:status, :eq, "verified"}]}]
        )

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()

      # Should return: age_only, status_only, both (but not neither)
      expected_ids = [exec_age_only.id, exec_status_only.id, exec_both.id] |> Enum.sort()

      assert result_ids == expected_ids
      assert length(results) == 3

      # Verify neither is not included
      refute exec_neither.id in result_ids
    end

    test "OR with three conditions" do
      # TODO: Implement
      # Test multi-condition OR (3+ filters)
    end

    test "OR with different operators" do
      # TODO: Implement
      # Test OR with :eq, :gt, :is_nil, :contains, :in, etc.
    end

    test "OR with :is_nil operator" do
      # TODO: Implement
      # Special case: :is_nil requires LEFT JOIN, test in OR context
    end

    test "OR with :is_not_nil operator" do
      # TODO: Implement
    end

    test "OR with :contains operator" do
      # TODO: Implement
    end

    test "OR with :list_contains operator" do
      # TODO: Implement
    end

    test "OR returns no results when no conditions match" do
      # TODO: Implement
    end

    test "OR returns all matching executions (no duplicates)" do
      # TODO: Implement
      # Ensure execution matching multiple OR conditions appears only once
    end
  end

  describe "AND operator (explicit)" do
    test "simple explicit AND with two conditions" do
      # TODO: Implement
      # Verify {:and, [f1, f2]} works same as [f1, f2]
    end

    test "explicit AND with three conditions" do
      # TODO: Implement
    end

    test "explicit AND returns only executions matching all conditions" do
      # TODO: Implement
    end
  end

  describe "mixed syntax: simple filters with logical operators" do
    test "simple filter AND OR block" do
      graph = contact_graph(random_string())

      # Create various combinations
      exec_active_email =
        Journey.start_execution(graph)
        |> Journey.set(:status, "active")
        |> Journey.set(:email, "user@example.com")

      exec_active_phone =
        Journey.start_execution(graph)
        |> Journey.set(:status, "active")
        |> Journey.set(:phone, "555-1234")

      exec_active_both =
        Journey.start_execution(graph)
        |> Journey.set(:status, "active")
        |> Journey.set(:email, "both@example.com")
        |> Journey.set(:phone, "555-5678")

      exec_active_neither =
        Journey.start_execution(graph) |> Journey.set(:status, "active")

      exec_inactive_email =
        Journey.start_execution(graph)
        |> Journey.set(:status, "inactive")
        |> Journey.set(:email, "inactive@example.com")

      # Filter: status='active' AND (email IS NOT NULL OR phone IS NOT NULL)
      results =
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [
            {:status, :eq, "active"},
            {:or, [{:email, :is_not_nil}, {:phone, :is_not_nil}]}
          ]
        )

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()

      # Should return: active_email, active_phone, active_both
      expected_ids = [exec_active_email.id, exec_active_phone.id, exec_active_both.id] |> Enum.sort()

      assert result_ids == expected_ids
      assert length(results) == 3

      # Verify excluded executions
      refute exec_active_neither.id in result_ids
      refute exec_inactive_email.id in result_ids
    end

    test "OR block AND simple filter" do
      # TODO: Implement
      # filter_by: [{:or, [...]}, {:status, :eq, "active"}]
    end

    test "multiple simple filters with OR in middle" do
      # TODO: Implement
      # filter_by: [f1, {:or, [f2, f3]}, f4]
      # Assert: f1 AND (f2 OR f3) AND f4
    end

    test "multiple OR blocks with simple filters" do
      # TODO: Implement
      # filter_by: [f1, {:or, [f2, f3]}, {:or, [f4, f5]}, f6]
    end
  end

  describe "nested logical operators" do
    test "OR inside AND" do
      graph = basic_graph(random_string())

      # Create test data
      # age >= 18 AND (status = 'verified' OR status = 'premium')
      exec_adult_verified =
        Journey.start_execution(graph)
        |> Journey.set(:age, 25)
        |> Journey.set(:status, "verified")

      exec_adult_premium =
        Journey.start_execution(graph)
        |> Journey.set(:age, 30)
        |> Journey.set(:status, "premium")

      exec_adult_pending =
        Journey.start_execution(graph)
        |> Journey.set(:age, 22)
        |> Journey.set(:status, "pending")

      exec_minor_verified =
        Journey.start_execution(graph)
        |> Journey.set(:age, 15)
        |> Journey.set(:status, "verified")

      # Filter: age >= 18 AND (status = 'verified' OR status = 'premium')
      results =
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [
            {:age, :gte, 18},
            {:or, [{:status, :eq, "verified"}, {:status, :eq, "premium"}]}
          ]
        )

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [exec_adult_verified.id, exec_adult_premium.id] |> Enum.sort()

      assert result_ids == expected_ids
      assert length(results) == 2

      # Verify excluded
      refute exec_adult_pending.id in result_ids
      refute exec_minor_verified.id in result_ids
    end

    test "AND inside OR" do
      # TODO: Implement
      # {:or, [f1, {:and, [f2, f3]}]}
    end

    test "OR inside AND inside OR (3 levels)" do
      # TODO: Implement
      # {:or, [f1, {:and, [f2, {:or, [f3, f4]}]}]}
    end

    test "AND inside OR inside AND (3 levels)" do
      # TODO: Implement
      # {:and, [f1, {:or, [f2, {:and, [f3, f4]}]}]}
    end

    test "deep nesting (4+ levels)" do
      # TODO: Implement
      # Test arbitrary depth to verify unlimited nesting
      # {:and, [{:or, [{:and, [{:or, [{:and, [f1, f2]}, f3]}]}, f4]}]}
    end

    test "complex real-world scenario: user eligibility" do
      # TODO: Implement
      # Realistic nested filter:
      # - Must be active
      # - Must have verified contact (email OR phone)
      # - Must be eligible (adult OR has parental consent)
      # filter_by: [
      #   {:status, :eq, "active"},
      #   {:or, [{:email_verified, :eq, true}, {:phone_verified, :eq, true}]},
      #   {:or, [{:age, :gte, 18}, {:parental_consent, :eq, true}]}
      # ]
    end
  end

  describe "edge cases" do
    test "single-item OR" do
      # TODO: Implement
      # {:or, [filter]} should work (may optimize to just filter)
    end

    test "single-item AND" do
      # TODO: Implement
      # {:and, [filter]} should work
    end

    test "duplicate filters in OR" do
      # TODO: Implement
      # {:or, [{:age, :gt, 18}, {:age, :gt, 18}]} should work (redundant but legal)
    end

    test "OR with no matching executions" do
      # TODO: Implement
      # Should return empty list
    end

    test "OR with all executions matching" do
      # TODO: Implement
      # Should return all
    end
  end

  describe "validation and error handling" do
    test "empty OR list raises error" do
      graph = basic_graph(random_string())

      assert_raise ArgumentError, ~r/Empty :or filter list/, fn ->
        Journey.list_executions(graph_name: graph.name, filter_by: [{:or, []}])
      end
    end

    test "empty AND list raises error" do
      graph = basic_graph(random_string())

      assert_raise ArgumentError, ~r/Empty :and filter list/, fn ->
        Journey.list_executions(graph_name: graph.name, filter_by: [{:and, []}])
      end
    end

    test "OR with non-list argument raises error" do
      # TODO: Implement
      # {:or, {:age, :gt, 18}} should raise ArgumentError
    end

    test "invalid logical operator raises error" do
      # TODO: Implement
      # {:nand, [...]} should raise ArgumentError
    end

    test "OR with invalid leaf filter raises error" do
      # TODO: Implement
      # {:or, [{:age, :invalid_op, 18}]} should raise ArgumentError
    end

    test "nested invalid structure raises error with clear message" do
      # TODO: Implement
      # Error messages should indicate where in the nesting the error occurred
    end
  end

  describe "backward compatibility" do
    test "simple list of filters works unchanged (implicit AND)" do
      graph = basic_graph(random_string())

      # Create test data
      exec_match = Journey.start_execution(graph) |> Journey.set(:age, 25) |> Journey.set(:status, "active")
      exec_age_only = Journey.start_execution(graph) |> Journey.set(:age, 30) |> Journey.set(:status, "inactive")
      exec_status_only = Journey.start_execution(graph) |> Journey.set(:age, 15) |> Journey.set(:status, "active")

      # Old syntax (implicit AND) - should still work
      results =
        Journey.list_executions(
          graph_name: graph.name,
          filter_by: [{:age, :gte, 18}, {:status, :eq, "active"}]
        )

      result_ids = Enum.map(results, & &1.id)

      # Should only return the one matching both conditions
      assert result_ids == [exec_match.id]
      assert length(results) == 1

      # Verify others excluded
      refute exec_age_only.id in result_ids
      refute exec_status_only.id in result_ids
    end

    test "all existing operators work with OR" do
      # TODO: Implement
      # Test each operator (:eq, :neq, :lt, :lte, :gt, :gte, :in, :not_in,
      # :contains, :icontains, :list_contains, :is_nil, :is_not_nil) in OR context
    end

    test "combining with other list_executions options" do
      # TODO: Implement
      # Verify OR/AND works with sort_by, limit, offset, graph_name, etc.
      # Journey.list_executions(
      #   graph_name: "users",
      #   filter_by: [{:or, [f1, f2]}],
      #   sort_by: [:age],
      #   limit: 10
      # )
    end
  end

  describe "performance and query structure" do
    test "OR uses single query (not multiple queries)" do
      # TODO: Implement
      # Verify through query logging that only one SQL query is executed
      # Not multiple list_executions calls
    end

    test "complex nested OR generates valid SQL" do
      # TODO: Implement
      # Could use Ecto debug logging to verify query structure
      # Should use EXISTS subqueries for OR
    end

    test "top-level AND continues to use JOIN optimization" do
      # TODO: Implement
      # Verify that simple filter lists still use JOINs, not EXISTS
      # Performance should not regress for common case
    end
  end

  describe "integration with value filtering" do
    test "OR with numeric comparisons" do
      # TODO: Implement
      # {:or, [{:age, :lt, 18}, {:age, :gt, 65}]}
    end

    test "OR with string matching" do
      # TODO: Implement
      # {:or, [{:email, :contains, "@gmail"}, {:email, :contains, "@yahoo"}]}
    end

    test "OR with list containment" do
      # TODO: Implement
      # {:or, [{:tags, :list_contains, "urgent"}, {:tags, :list_contains, "priority"}]}
    end

    test "OR combining different value types" do
      # TODO: Implement
      # {:or, [{:age, :gt, 18}, {:status, :eq, "verified"}, {:tags, :list_contains, "admin"}]}
    end
  end

  # Helper functions

  defp basic_graph(test_id) do
    Journey.new_graph(
      "or_and_test_basic_#{test_id}",
      "1.0.0",
      [
        input(:age),
        input(:status),
        input(:email),
        input(:phone)
      ]
    )
  end

  defp contact_graph(test_id) do
    Journey.new_graph(
      "or_and_test_contact_#{test_id}",
      "1.0.0",
      [
        input(:status),
        input(:email),
        input(:email_verified),
        input(:phone),
        input(:phone_verified)
      ]
    )
  end
end
