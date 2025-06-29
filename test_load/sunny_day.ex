defmodule LoadTest.SunnyDay do
  @moduledoc """
  This is a standalone load test script for Journey execution system.

  This script creates concurrent executions following different trajectories
  to validate system behavior under load.

  Usage example:
  ```elixir
  iex> r = LoadTest.SunnyDay.run(5)
  iex> r.success
  true
  iex> r.metrics.number_of_executions
  5
  ```
  """

  require Logger

  def run(total_executions) do
    graph = Journey.Examples.CreditCardApplication.graph()

    executions = for _i <- 1..total_executions, do: Journey.start_execution(graph)

    initial_memory = get_memory_usage_mb()
    Logger.info("Initial memory usage: #{initial_memory} MB")

    start_time = System.monotonic_time(:second)

    tasks =
      for e <- executions do
        Task.async(fn ->
          try do
            e = lifetime_success_flow(e)

            e
            |> Journey.get_value(:archive)
            |> case do
              {:ok, _} ->
                Logger.debug("#{e.id}: execution completed successfully.")

              _huh ->
                Logger.error("#{e.id}: execution failed.")
                Journey.Tools.summarize(e.id) |> IO.puts()
            end
          rescue
            exception ->
              Logger.error("#{inspect(e.id)}: execution raised an exception: #{inspect(exception)}")
              Journey.Tools.summarize(e.id) |> IO.puts()
          end
        end)
      end

    Task.await_many(tasks, 300_000)
    end_time = System.monotonic_time(:second)

    final_memory = get_memory_usage_mb()
    Logger.info("memory usage: #{initial_memory} -> #{final_memory} MB")

    %{
      success: true,
      metrics: %{
        duration_seconds: end_time - start_time,
        number_of_executions: total_executions,
        initial_memory: initial_memory,
        final_memory: final_memory,
        peak_memory: nil
      }
    }
  end

  defp get_memory_usage_mb() do
    total_memory = :erlang.memory(:total)
    total_memory / (1024 * 1024)
  end

  def lifetime_success_flow(e) do
    Logger.info("lifetime_success_flow[#{e.id}]: starting")

    e =
      e
      |> Journey.set_value(:full_name, "Mario")
      |> Journey.set_value(:birth_date, "10/11/1981")
      |> Journey.set_value(:ssn, "123-45-6789")
      |> Journey.set_value(:email_address, "mario@example.com")

    Process.sleep(4_000)

    {:ok, _credit_score} = e |> Journey.get_value(:credit_score, wait: 45_000)
    {:ok, _decision} = e |> Journey.get_value(:preapproval_decision, wait: 45_000)
    {:ok, true} = e |> Journey.get_value(:preapproval_process_completed, wait: 45_000)
    {:ok, true} = e |> Journey.get_value(:send_preapproval_reminder, wait: 45_000)
    e = e |> Journey.set_value(:credit_card_requested, true)
    {:ok, true} = e |> Journey.get_value(:initiate_credit_card_issuance, wait: 45_000)
    e = e |> Journey.set_value(:credit_card_mailed, true)
    {:ok, true} = e |> Journey.get_value(:credit_card_mailed_notification, wait: 45_000)
    {:ok, _} = e |> Journey.get_value(:archive, wait: 45_000)
    Logger.info("lifetime_success_flow[#{e.id}]: completed")

    e
  end
end
