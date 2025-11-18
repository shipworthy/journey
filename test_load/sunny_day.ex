defmodule LoadTest.SunnyDay do
  @moduledoc false

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
            |> Journey.get(:archive)
            |> case do
              {:ok, _, _} ->
                Logger.debug("#{e.id}: execution completed successfully.")

              _huh ->
                Logger.error("#{e.id}: execution failed.")
                Journey.Tools.introspect(e.id) |> IO.puts()
            end
          rescue
            exception ->
              Logger.error("#{inspect(e.id)}: execution raised an exception: #{inspect(exception)}")
              Journey.Tools.introspect(e.id) |> IO.puts()
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
      |> Journey.set(:full_name, "Mario")
      |> Journey.set(:birth_date, "10/11/1981")
      |> Journey.set(:ssn, "123-45-6789")
      |> Journey.set(:email_address, "mario@example.com")

    Process.sleep(4_000)

    {:ok, _credit_score, _} = e |> Journey.get(:credit_score, wait: :any, timeout: 45_000)
    {:ok, _decision, _} = e |> Journey.get(:preapproval_decision, wait: :any, timeout: 45_000)
    {:ok, true, _} = e |> Journey.get(:preapproval_process_completed, wait: :any, timeout: 45_000)
    {:ok, true, _} = e |> Journey.get(:send_preapproval_reminder, wait: :any, timeout: 45_000)
    e = e |> Journey.set(:credit_card_requested, true)
    {:ok, true, _} = e |> Journey.get(:initiate_credit_card_issuance, wait: :any, timeout: 45_000)
    e = e |> Journey.set(:credit_card_mailed, true)
    {:ok, true, _} = e |> Journey.get(:credit_card_mailed_notification, wait: :any, timeout: 45_000)
    {:ok, _, _} = e |> Journey.get(:archive, wait: :any, timeout: 45_000)
    Logger.info("lifetime_success_flow[#{e.id}]: completed")

    e
  end
end
