defmodule Journey.JourneyGetValueNewOptionsTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "get_value with new wait: and timeout: options" do
    setup do
      test_id = random_string()
      execution = basic_graph(test_id) |> Journey.start_execution()
      {:ok, execution: execution}
    end

    test "wait: :immediate returns immediately (default behavior)", %{execution: execution} do
      # Test default behavior (no wait option)
      assert Journey.get_value(execution, :first_name) == {:error, :not_set}

      # Test explicit :immediate
      assert Journey.get_value(execution, :first_name, wait: :immediate) == {:error, :not_set}
    end

    test "wait: :any waits for value to be set", %{execution: execution} do
      # Set value in background process
      test_pid = self()

      spawn(fn ->
        Process.sleep(100)
        updated_execution = execution |> Journey.set(:first_name, "Mario")
        send(test_pid, {:value_set, updated_execution})
      end)

      # Should wait for the value
      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait: :any)

      # Verify the task completed
      assert_receive {:value_set, _}, 1000
    end

    test "wait: :any with custom timeout", %{execution: execution} do
      # Should timeout after 100ms since no value is set
      {:error, :not_set} = Journey.get_value(execution, :first_name, wait: :any, timeout: 100)
    end

    test "wait: :any with infinity timeout", %{execution: execution} do
      # Set value in background after short delay
      test_pid = self()

      spawn(fn ->
        Process.sleep(50)
        updated_execution = execution |> Journey.set(:first_name, "Mario")
        send(test_pid, {:value_set, updated_execution})
      end)

      # Should wait indefinitely and get the value
      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait: :any, timeout: :infinity)
      assert_receive {:value_set, _}, 1000
    end

    test "wait: :newer waits for newer revision than current execution", %{execution: execution} do
      # Set initial value
      execution = execution |> Journey.set(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution, :first_name)

      # Update value in background
      test_pid = self()

      spawn(fn ->
        Process.sleep(100)
        updated_execution = execution |> Journey.set(:first_name, "Luigi")
        send(test_pid, {:updated, updated_execution})
      end)

      # Should wait for newer revision and get updated value
      {:ok, "Luigi"} = Journey.get_value(execution, :first_name, wait: :newer)
      assert_receive {:updated, _}, 1000
    end

    test "wait: {:newer_than, revision} waits for specific revision", %{execution: execution} do
      # Set initial value (revision will be 1)
      execution = execution |> Journey.set(:first_name, "Mario")

      # Should return immediately since current revision (1) is already > 0
      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait: {:newer_than, 0})

      # Should timeout since no revision > 10 exists
      {:error, :not_set} = Journey.get_value(execution, :first_name, wait: {:newer_than, 10}, timeout: 100)
    end

    test "wait: :newer with first value when none exists", %{execution: execution} do
      # Set value in background
      test_pid = self()

      spawn(fn ->
        Process.sleep(100)
        updated_execution = execution |> Journey.set(:first_name, "Mario")
        send(test_pid, {:value_set, updated_execution})
      end)

      # Should wait for first value to be set
      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait: :newer)
      assert_receive {:value_set, _}, 1000
    end

    test "computed node with wait: :any", %{execution: execution} do
      # Set dependency which should trigger computation
      execution = execution |> Journey.set(:first_name, "Mario")

      # Should wait for computation to complete
      {:ok, "Hello, Mario"} = Journey.get_value(execution, :greeting, wait: :any)
    end

    test "invalid wait option raises error", %{execution: execution} do
      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get_value(execution, :first_name, wait: :invalid_option)
      end

      assert_raise ArgumentError, ~r/Invalid :wait option/, fn ->
        Journey.get_value(execution, :first_name, wait: {:newer_than, "not_integer"})
      end
    end

    test "mixing new and old style options raises error", %{execution: execution} do
      assert_raise ArgumentError, ~r/Cannot mix new style options/, fn ->
        Journey.get_value(execution, :first_name, wait: :any, wait_any: true)
      end

      assert_raise ArgumentError, ~r/Cannot mix new style options/, fn ->
        Journey.get_value(execution, :first_name, wait: :newer, wait_new: true)
      end

      assert_raise ArgumentError, ~r/Cannot mix new style options/, fn ->
        Journey.get_value(execution, :first_name, timeout: 5000, wait_any: true)
      end
    end

    test "old style options still work (backwards compatibility)", %{execution: execution} do
      # Test wait_any still works
      test_pid = self()

      spawn(fn ->
        Process.sleep(100)
        updated_execution = execution |> Journey.set(:first_name, "Mario")
        send(test_pid, {:value_set, updated_execution})
      end)

      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait_any: true)
      assert_receive {:value_set, _}, 1000
    end

    test "timeout option only works with wait option", %{execution: execution} do
      # Setting just timeout without wait should work (timeout is ignored)
      {:error, :not_set} = Journey.get_value(execution, :first_name, timeout: 5000)
    end
  end

  defp basic_graph(test_id) do
    Journey.new_graph(
      "new options test graph #{__MODULE__} #{test_id}",
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
end
