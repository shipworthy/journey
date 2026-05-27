defmodule Journey.Scheduler.MutateMultiTargetTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  alias Journey.Persistence.Schema.Execution.Computation

  describe "multi-target mutate |" do
    test "list mutate writes the same value to every target (default revision behavior)" do
      graph =
        Journey.new_graph("multi_target_fanout_#{random_string()}", "v1.0.0", [
          input(:a),
          input(:b),
          input(:c),
          input(:trigger),
          mutate(:scrub, [:trigger], fn _ -> {:ok, "<REDACTED>"} end, mutates: [:a, :b, :c])
        ])

      execution =
        graph
        |> Journey.start()
        |> Journey.set(:a, "secret-a")
        |> Journey.set(:b, "secret-b")
        |> Journey.set(:c, "secret-c")
        |> Journey.set(:trigger, "go")

      # The mutate node's own marker value pins the full list shape, guarding against an
      # accidental List.wrap before the marker write.
      assert {:ok, "updated [:a, :b, :c]", _rev} = Journey.get(execution, :scrub, wait: :any)

      # The single returned value was fanned out to every target.
      assert {:ok, "<REDACTED>", _} = Journey.get(execution, :a)
      assert {:ok, "<REDACTED>", _} = Journey.get(execution, :b)
      assert {:ok, "<REDACTED>", _} = Journey.get(execution, :c)
    end

    test "single-atom mutate is unchanged (regression guard)" do
      graph =
        Journey.new_graph("multi_target_single_#{random_string()}", "v1.0.0", [
          input(:name),
          input(:trigger),
          mutate(:scrub, [:trigger], fn _ -> {:ok, "<REDACTED>"} end, mutates: :name)
        ])

      execution =
        graph
        |> Journey.start()
        |> Journey.set(:name, "Mario")
        |> Journey.set(:trigger, "go")

      assert {:ok, "updated :name", _rev} = Journey.get(execution, :scrub, wait: :any)
      assert {:ok, "<REDACTED>", _} = Journey.get(execution, :name)
    end

    test "list mutate with update_revision_on_change: true bumps changed targets to one revision and re-fires downstream" do
      graph =
        Journey.new_graph("multi_target_revbump_#{random_string()}", "v1.0.0", [
          input(:a),
          input(:b),
          input(:trigger),
          mutate(:bump, [:trigger], fn _ -> {:ok, "new"} end,
            mutates: [:a, :b],
            update_revision_on_change: true
          ),
          compute(:derived_a, [:a], fn %{a: a} -> {:ok, "derived:#{a}"} end),
          compute(:derived_b, [:b], fn %{b: b} -> {:ok, "derived:#{b}"} end)
        ])

      execution =
        graph
        |> Journey.start()
        |> Journey.set(:a, "old")
        |> Journey.set(:b, "old")

      {:ok, "derived:old", da_rev0} = Journey.get(execution, :derived_a, wait: :any)
      {:ok, "derived:old", db_rev0} = Journey.get(execution, :derived_b, wait: :any)

      execution = Journey.set(execution, :trigger, "go")
      {:ok, "updated [:a, :b]", _} = Journey.get(execution, :bump, wait: :any)

      # Both targets changed "old" -> "new", written at the same new revision in one transaction.
      assert {:ok, "new", rev_a} = Journey.get(execution, :a)
      assert {:ok, "new", rev_b} = Journey.get(execution, :b)
      assert rev_a == rev_b

      # The revision bump re-fires each target's downstream compute.
      assert {:ok, "derived:new", _} = Journey.get(execution, :derived_a, wait: {:newer_than, da_rev0})
      assert {:ok, "derived:new", _} = Journey.get(execution, :derived_b, wait: {:newer_than, db_rev0})
    end

    test "list mutate with update_revision_on_change: true skips targets whose value is unchanged" do
      graph =
        Journey.new_graph("multi_target_skip_#{random_string()}", "v1.0.0", [
          input(:changed),
          input(:unchanged),
          input(:trigger),
          mutate(:sync, [:trigger], fn _ -> {:ok, "same"} end,
            mutates: [:changed, :unchanged],
            update_revision_on_change: true
          )
        ])

      execution =
        graph
        |> Journey.start()
        |> Journey.set(:changed, "different")
        |> Journey.set(:unchanged, "same")

      {:ok, "different", changed_rev_before} = Journey.get(execution, :changed)
      {:ok, "same", unchanged_rev_before} = Journey.get(execution, :unchanged)

      execution = Journey.set(execution, :trigger, "go")
      {:ok, "updated [:changed, :unchanged]", _} = Journey.get(execution, :sync, wait: :any)

      # :changed actually changed -> value and revision updated.
      assert {:ok, "same", changed_rev_after} = Journey.get(execution, :changed)
      assert changed_rev_after > changed_rev_before

      # :unchanged already equaled "same" -> skipped entirely, revision untouched.
      assert {:ok, "same", unchanged_rev_after} = Journey.get(execution, :unchanged)
      assert unchanged_rev_after == unchanged_rev_before
    end

    test "a failing list mutate writes no target (all-or-nothing)" do
      graph =
        Journey.new_graph("multi_target_fail_#{random_string()}", "v1.0.0", [
          input(:a),
          input(:b),
          input(:c),
          input(:trigger),
          mutate(:scrub, [:trigger], fn _ -> {:error, "boom"} end,
            mutates: [:a, :b, :c],
            max_retries: 0
          )
        ])

      execution =
        graph
        |> Journey.start()
        |> Journey.set(:trigger, "go")

      # max_retries: 0 makes the first {:error, _} terminal. Failure doesn't write to the
      # values table, so poll the Computation table for the :failed row.
      _failed = wait_for_mutate_failed(execution.id, "scrub", 10_000)

      # No partial writes: every target is still unset, earning the all-or-nothing claim by
      # checking each target rather than just one.
      assert {:error, :not_set} = Journey.get(execution, :a)
      assert {:error, :not_set} = Journey.get(execution, :b)
      assert {:error, :not_set} = Journey.get(execution, :c)
    end

    test "mermaid renders a list of mutates targets readably" do
      graph =
        Journey.new_graph("multi_target_mermaid_#{random_string()}", "v1", [
          input(:a),
          input(:b),
          input(:trigger),
          mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end, mutates: [:a, :b])
        ])

      execution = Journey.start(graph)
      mermaid = Journey.Tools.generate_mermaid_execution(execution.id)
      assert mermaid =~ "mutates: a, b"
    end
  end

  describe "multi-target mutate validation |" do
    test "empty :mutates list raises ArgumentError (bad shape, at construction)" do
      assert_raise ArgumentError, ~r/non-empty list of atoms/, fn ->
        mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end, mutates: [])
      end
    end

    test "non-atom element in :mutates raises ArgumentError (bad shape, at construction)" do
      assert_raise ArgumentError, ~r/non-empty list of atoms/, fn ->
        mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end, mutates: [:a, "b"])
      end
    end

    test "duplicate target in :mutates list raises ArgumentError, naming the duplicate" do
      assert_raise ArgumentError, ~r/must not list a node more than once.*:a/, fn ->
        mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end, mutates: [:a, :b, :a])
      end
    end

    test "non-boolean update_revision_on_change raises ArgumentError at construction" do
      assert_raise ArgumentError, ~r/update_revision_on_change must be a boolean/, fn ->
        mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end, mutates: [:a], update_revision_on_change: :yes)
      end
    end

    test "unknown target in list raises at new_graph, naming the offending element" do
      assert_raise RuntimeError, ~r/mutates an unknown node ':nope'/, fn ->
        Journey.new_graph("multi_target_unknown_#{random_string()}", "v1.0.0", [
          input(:a),
          input(:trigger),
          mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end, mutates: [:a, :nope])
        ])
      end
    end

    test "self in target list raises at new_graph" do
      assert_raise RuntimeError, ~r/attempts to mutate itself/, fn ->
        Journey.new_graph("multi_target_self_#{random_string()}", "v1.0.0", [
          input(:a),
          input(:trigger),
          mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end, mutates: [:a, :scrub])
        ])
      end
    end

    test "upstream cycle with update_revision_on_change: true raises at new_graph, naming the element" do
      assert_raise RuntimeError, ~r/creates a cycle by mutating ':trigger'/, fn ->
        Journey.new_graph("multi_target_cycle_#{random_string()}", "v1.0.0", [
          input(:trigger),
          input(:other),
          mutate(:scrub, [:trigger], fn _ -> {:ok, "x"} end,
            mutates: [:other, :trigger],
            update_revision_on_change: true
          )
        ])
      end
    end
  end

  defp wait_for_mutate_failed(execution_id, node_name, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll_mutate_failed(execution_id, node_name, deadline)
  end

  defp poll_mutate_failed(execution_id, node_name, deadline) do
    row =
      from(c in Computation,
        where:
          c.execution_id == ^execution_id and
            c.node_name == ^node_name and
            c.computation_type == :mutate and
            c.state == :failed,
        order_by: [desc: c.inserted_at],
        limit: 1
      )
      |> Journey.Repo.one()

    cond do
      row != nil ->
        row

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("mutate node #{node_name} did not reach :failed within deadline")

      true ->
        Process.sleep(50)
        poll_mutate_failed(execution_id, node_name, deadline)
    end
  end
end
