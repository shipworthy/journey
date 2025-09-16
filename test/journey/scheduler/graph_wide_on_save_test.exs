defmodule Journey.Scheduler.GraphWideOnSaveTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  import Journey.Node

  require Logger

  describe "graph-wide f_on_save" do
    test "graph-wide f_on_save is called for all compute nodes" do
      {_result, log} =
        with_log(fn ->
          graph =
            Journey.new_graph(
              "graph with global f_on_save #{__MODULE__}",
              "1.0.0",
              [
                input(:x),
                input(:y),
                compute(:sum, [:x, :y], fn %{x: x, y: y} ->
                  {:ok, x + y}
                end),
                compute(:product, [:x, :y], fn %{x: x, y: y} ->
                  {:ok, x * y}
                end)
              ],
              f_on_save: fn _execution_id, node_name, result ->
                Logger.error("GRAPH_WIDE: #{node_name} => #{inspect(result)}")
                :ok
              end
            )

          execution = Journey.start_execution(graph)
          execution = Journey.set(execution, :x, 5)
          execution = Journey.set(execution, :y, 3)

          assert Journey.get_value(execution, :sum, wait_any: true) == {:ok, 8}
          assert Journey.get_value(execution, :product, wait_any: true) == {:ok, 15}

          # Wait for async callbacks to complete
          Process.sleep(2000)
        end)

      assert log =~ "GRAPH_WIDE: sum => {:ok, 8}"
      assert log =~ "GRAPH_WIDE: product => {:ok, 15}"
    end

    test "node-specific f_on_save takes precedence but both are called" do
      {_result, log} =
        with_log(fn ->
          graph =
            Journey.new_graph(
              "graph with both callbacks #{__MODULE__}",
              "1.0.0",
              [
                input(:name),
                compute(
                  :greeting,
                  [:name],
                  fn %{name: name} ->
                    {:ok, "Hello, #{name}"}
                  end,
                  f_on_save: fn _execution_id, result ->
                    Logger.error("NODE_SPECIFIC: greeting => #{inspect(result)}")
                    :ok
                  end
                ),
                compute(
                  :uppercase,
                  [:name],
                  fn %{name: name} ->
                    {:ok, String.upcase(name)}
                  end
                  # No node-specific f_on_save
                )
              ],
              f_on_save: fn _execution_id, node_name, result ->
                Logger.error("GRAPH_WIDE: #{node_name} => #{inspect(result)}")
                :ok
              end
            )

          execution = Journey.start_execution(graph)
          execution = Journey.set(execution, :name, "Alice")

          assert Journey.get_value(execution, :greeting, wait_any: true) == {:ok, "Hello, Alice"}
          assert Journey.get_value(execution, :uppercase, wait_any: true) == {:ok, "ALICE"}

          # Wait for async callbacks to complete
          Process.sleep(2000)
        end)

      # Both callbacks should be called for :greeting
      assert log =~ "NODE_SPECIFIC: greeting => {:ok, \"Hello, Alice\"}"
      assert log =~ "GRAPH_WIDE: greeting => {:ok, \"Hello, Alice\"}"

      # Only graph-wide should be called for :uppercase
      assert log =~ "GRAPH_WIDE: uppercase => {:ok, \"ALICE\"}"
      refute log =~ "NODE_SPECIFIC: uppercase"
    end

    test "graph-wide f_on_save works with mutate nodes" do
      {_result, log} =
        with_log(fn ->
          graph =
            Journey.new_graph(
              "graph with mutate and global f_on_save #{__MODULE__}",
              "1.0.0",
              [
                input(:sensitive_data),
                mutate(
                  :redact_data,
                  [:sensitive_data],
                  fn %{sensitive_data: _data} ->
                    {:ok, "[REDACTED]"}
                  end,
                  mutates: :sensitive_data
                )
              ],
              f_on_save: fn _execution_id, node_name, result ->
                Logger.error("GRAPH_WIDE: #{node_name} => #{inspect(result)}")
                :ok
              end
            )

          execution = Journey.start_execution(graph)
          execution = Journey.set(execution, :sensitive_data, "SSN: 123-45-6789")

          assert Journey.get_value(execution, :redact_data, wait_any: true) == {:ok, "updated :sensitive_data"}
          assert Journey.get_value(execution, :sensitive_data, wait_any: true) == {:ok, "[REDACTED]"}

          # Wait for async callbacks to complete
          Process.sleep(2000)
        end)

      # Mutate nodes return "updated :node_name" as their result
      assert log =~ "GRAPH_WIDE: redact_data"
    end

    test "errors in graph-wide f_on_save don't affect computation" do
      {_result, log} =
        with_log(fn ->
          graph =
            Journey.new_graph(
              "graph with failing f_on_save #{__MODULE__}",
              "1.0.0",
              [
                input(:value),
                compute(:double, [:value], fn %{value: v} ->
                  {:ok, v * 2}
                end)
              ],
              f_on_save: fn _execution_id, _node_name, _result ->
                raise "Intentional error in graph-wide f_on_save"
              end
            )

          execution = Journey.start_execution(graph)
          execution = Journey.set(execution, :value, 10)

          # Computation should succeed despite callback error
          assert Journey.get_value(execution, :double, wait_any: true) == {:ok, 20}

          # Wait for async callbacks to complete
          Process.sleep(2000)
        end)

      assert log =~ "graph-wide f_on_save raised an exception"
      assert log =~ "Intentional error in graph-wide f_on_save"
    end

    test "graph-wide f_on_save receives correct parameters" do
      test_pid = self()

      graph =
        Journey.new_graph(
          "graph testing parameters #{__MODULE__}",
          "1.0.0",
          [
            input(:input_value),
            compute(:compute_node, [:input_value], fn %{input_value: v} ->
              {:ok, "computed: #{v}"}
            end)
          ],
          f_on_save: fn execution_id, node_name, result ->
            send(test_pid, {:callback_called, execution_id, node_name, result})
            :ok
          end
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :input_value, "test")

      assert Journey.get_value(execution, :compute_node, wait_any: true) == {:ok, "computed: test"}

      # Wait for the callback message
      assert_receive {:callback_called, exec_id, :compute_node, {:ok, "computed: test"}}, 3000
      assert exec_id == execution.id
    end

    test "graph-wide f_on_save is not called for input nodes" do
      {_result, log} =
        with_log(fn ->
          graph =
            Journey.new_graph(
              "graph testing input nodes #{__MODULE__}",
              "1.0.0",
              [
                input(:test_input),
                compute(:test_compute, [:test_input], fn %{test_input: v} ->
                  {:ok, v}
                end)
              ],
              f_on_save: fn _execution_id, node_name, _result ->
                Logger.error("GRAPH_WIDE called for: #{node_name}")
                :ok
              end
            )

          execution = Journey.start_execution(graph)
          execution = Journey.set(execution, :test_input, "value")

          assert Journey.get_value(execution, :test_compute, wait_any: true) == {:ok, "value"}

          # Wait for async callbacks to complete
          Process.sleep(2000)
        end)

      # Should only be called for compute node, not input node
      assert log =~ "GRAPH_WIDE called for: test_compute"
      refute log =~ "GRAPH_WIDE called for: test_input"
    end

    test "graph-wide f_on_save works with schedule_once nodes" do
      {_result, log} =
        with_log(fn ->
          graph =
            Journey.new_graph(
              "graph with schedule_once #{__MODULE__}",
              "1.0.0",
              [
                input(:trigger),
                schedule_once(
                  :scheduled_task,
                  [:trigger],
                  fn _params ->
                    # Schedule for 1 second in the future
                    {:ok, System.system_time(:second) + 1}
                  end,
                  f_on_save: fn _execution_id, result ->
                    Logger.error("NODE_SPECIFIC schedule: #{inspect(result)}")
                    :ok
                  end
                )
              ],
              f_on_save: fn _execution_id, node_name, result ->
                Logger.error("GRAPH_WIDE: #{node_name} => #{inspect(result)}")
                :ok
              end
            )

          execution = Journey.start_execution(graph)
          execution = Journey.set(execution, :trigger, true)

          # Get the scheduled time
          {:ok, _scheduled_time} = Journey.get_value(execution, :scheduled_task, wait_any: true)

          # Wait for async callbacks to complete
          Process.sleep(2000)
        end)

      # Both callbacks should be called
      assert log =~ "NODE_SPECIFIC schedule:"
      assert log =~ "GRAPH_WIDE: scheduled_task =>"
    end
  end

  describe "no f_on_save defined" do
    test "works without any f_on_save callbacks" do
      graph =
        Journey.new_graph(
          "graph without f_on_save #{__MODULE__}",
          "1.0.0",
          [
            input(:x),
            compute(:double, [:x], fn %{x: x} ->
              {:ok, x * 2}
            end)
          ]
          # No f_on_save option
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set(execution, :x, 5)

      assert Journey.get_value(execution, :double, wait_any: true) == {:ok, 10}
    end
  end
end
