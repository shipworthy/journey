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
      # Set value in background task
      Task.async(fn ->
        Journey.set(execution, :first_name, "Mario")
      end)

      # Should wait for the value
      assert {:ok, %{value: "Mario"}} = Journey.get_value(execution, :first_name, wait: :any)
    end

    test "wait: :any with custom timeout", %{execution: execution} do
      # Should timeout after 100ms since no value is set
      assert Journey.get_value(execution, :first_name, wait: :any, timeout: 100) == {:error, :not_set}
    end

    test "wait: :any with infinity timeout", %{execution: execution} do
      # Set value in background task after short delay
      Task.async(fn ->
        Process.sleep(50)
        Journey.set(execution, :first_name, "Mario")
      end)

      # Should wait indefinitely and get the value
      assert {:ok, %{value: "Mario"}} = Journey.get_value(execution, :first_name, wait: :any, timeout: :infinity)
    end

    test "wait: :newer waits for newer revision than current execution", %{execution: execution} do
      # Set initial value
      execution = execution |> Journey.set(:first_name, "Mario")
      assert {:ok, %{value: "Mario"}} = Journey.get_value(execution, :first_name)

      # Update value in background task
      Task.async(fn ->
        Journey.set(execution, :first_name, "Luigi")
      end)

      # Should wait for newer revision and get updated value
      assert {:ok, %{value: "Luigi"}} = Journey.get_value(execution, :first_name, wait: :newer)
    end

    test "wait: {:newer_than, revision} waits for specific revision", %{execution: execution} do
      # Set initial value (revision will be 1)
      execution = execution |> Journey.set(:first_name, "Mario")

      # Should return immediately since current revision (1) is already > 0
      assert {:ok, %{value: "Mario"}} = Journey.get_value(execution, :first_name, wait: {:newer_than, 0})

      # Should timeout since no revision > 10 exists
      assert Journey.get_value(execution, :first_name, wait: {:newer_than, 10}, timeout: 100) == {:error, :not_set}
    end

    test "wait: :newer with first value when none exists", %{execution: execution} do
      # Set value in background task
      Task.async(fn ->
        Journey.set(execution, :first_name, "Mario")
      end)

      # Should wait for first value to be set
      assert {:ok, %{value: "Mario"}} = Journey.get_value(execution, :first_name, wait: :newer)
    end

    test "computed node with wait: :any", %{execution: execution} do
      # Set dependency which should trigger computation
      execution = execution |> Journey.set(:first_name, "Mario")

      # Should wait for computation to complete
      assert {:ok, %{value: "Hello, Mario"}} = Journey.get_value(execution, :greeting, wait: :any)
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
      Task.async(fn ->
        Journey.set(execution, :first_name, "Mario")
      end)

      assert {:ok, %{value: "Mario"}} = Journey.get_value(execution, :first_name, wait_any: true)
    end

    test "timeout option only works with wait option", %{execution: execution} do
      # Setting just timeout without wait should work (timeout is ignored)
      assert Journey.get_value(execution, :first_name, timeout: 5000) == {:error, :not_set}
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
