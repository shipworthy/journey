defmodule Journey.JourneyWaitNewTest do
  use ExUnit.Case, async: true

  import Journey.Helpers.Random, only: [random_string: 0]

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "get_value with wait_new option" do
    setup do
      test_id = random_string()
      execution = basic_graph(test_id) |> Journey.start_execution()
      {:ok, execution: execution}
    end

    test "wait_new waits for new revision when value exists", %{execution: execution} do
      # Set initial value
      execution = execution |> Journey.set(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait_any: true)

      # Set new value and use wait_new to get the updated value
      _execution_v2 = execution |> Journey.set(:first_name, "Luigi")
      {:ok, "Luigi"} = Journey.get_value(execution, :first_name, wait_new: true)
    end

    test "wait_new waits for first value when no value exists", %{execution: execution} do
      # Start a task that will set the value after a delay
      test_pid = self()

      spawn(fn ->
        Process.sleep(100)
        updated_execution = execution |> Journey.set(:first_name, "Delayed Mario")
        send(test_pid, {:value_set, updated_execution})
      end)

      # This should wait for the first value to be set
      {:ok, "Delayed Mario"} = Journey.get_value(execution, :first_name, wait_new: true)

      # Verify the task completed
      assert_receive {:value_set, _}, 1000
    end

    test "wait_new timeout behavior", %{execution: execution} do
      # Set initial value
      execution = execution |> Journey.set(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait_any: true)

      # Try to wait for new revision with short timeout - should timeout
      {:error, :not_set} = Journey.get_value(execution, :first_name, wait_new: 100)
    end

    test "wait_new with concurrent updates", %{execution: execution} do
      # Set initial value
      execution = execution |> Journey.set(:first_name, "Mario")
      {:ok, "Mario"} = Journey.get_value(execution, :first_name, wait_any: true)

      test_pid = self()

      # Start multiple concurrent update tasks
      for i <- 1..3 do
        spawn(fn ->
          Process.sleep(50 * i)
          updated_execution = execution |> Journey.set(:first_name, "Update#{i}")
          send(test_pid, {:update_complete, i, updated_execution})
        end)
      end

      # wait_new should get one of the updates
      {:ok, value} = Journey.get_value(execution, :first_name, wait_new: true)
      assert String.starts_with?(value, "Update")

      # Wait for all tasks to complete
      for i <- 1..3 do
        assert_receive {:update_complete, ^i, _}, 1000
      end
    end

    test "wait_any and wait_new are mutually exclusive", %{execution: execution} do
      assert_raise ArgumentError, "Options :wait_any and :wait_new are mutually exclusive", fn ->
        Journey.get_value(execution, :first_name, wait_any: true, wait_new: true)
      end

      assert_raise ArgumentError, "Options :wait_any and :wait_new are mutually exclusive", fn ->
        Journey.get_value(execution, :first_name, wait_any: 5000, wait_new: 1000)
      end
    end

    test "wait_new works with dependent computations", %{execution: execution} do
      # Set initial first_name to trigger greeting computation
      execution = execution |> Journey.set(:first_name, "Mario")
      {:ok, "Hello, Mario", greeting_rev} = Journey.get(execution, :greeting, wait: :any)

      # Change first_name and wait for new greeting computation
      # Use explicit revision to avoid race where the execution returned by set
      # may already contain the recomputed value
      execution = execution |> Journey.set(:first_name, "Luigi")
      {:ok, "Hello, Luigi"} = Journey.get_value(execution, :greeting, wait: {:newer_than, greeting_rev})
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
end
