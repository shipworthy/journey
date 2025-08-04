defmodule LoadTest.PerformanceBenchmark do
  @moduledoc """
  Performance benchmark test for Journey database optimizations.

  This test measures the impact of database index optimizations on:
  - Scheduler query performance
  - Value lookup performance
  - Time-based query performance
  - Active execution filtering
  - Scheduled computation queries

  Usage:
  ```bash
  make test-performance
  # or
  mix run test_load/performance_benchmark.exs
  ```
  """

  require Logger
  import Ecto.Query

  @just_the_sweeps false
  # @just_the_sweeps true
  @scenarios if(@just_the_sweeps,
               do: [],
               else: [
                 :scheduler_stress,
                 :high_frequency_values,
                 :time_based_queries,
                 :archive_unarchive_workload,
                 :scheduled_computation_load
               ]
             ) ++
               [:sweeper]

  def run(opts \\ []) do
    concurrency = Keyword.get(opts, :concurrency, 20)
    iterations = Keyword.get(opts, :iterations, 50)

    IO.puts("Starting performance benchmark with #{concurrency} concurrent executions, #{iterations} iterations")

    initial_memory = get_memory_usage_mb()
    start_time = System.monotonic_time(:millisecond)

    # Run all test scenarios in parallel
    scenario_results =
      @scenarios
      |> Task.async_stream(
        fn scenario ->
          IO.puts("[#{scenario}]: Running scenario")
          scenario_start = System.monotonic_time(:millisecond)
          result = run_scenario(scenario, concurrency, iterations)
          scenario_end = System.monotonic_time(:millisecond)

          # Add timing to result
          duration_ms = scenario_end - scenario_start
          result_with_timing = Map.put(result, :duration_ms, duration_ms)
          IO.puts("[#{scenario}]: scenario completed after #{duration_ms} ms")
          {scenario, result_with_timing}
        end,
        timeout: 600_000
      )
      |> Enum.map(fn {:ok, result} -> result end)
      |> Map.new()

    end_time = System.monotonic_time(:millisecond)
    final_memory = get_memory_usage_mb()

    total_duration_ms = end_time - start_time

    results = %{
      success: true,
      total_duration_ms: total_duration_ms,
      memory: %{
        initial_mb: initial_memory,
        final_mb: final_memory,
        delta_mb: final_memory - initial_memory
      },
      database_metrics: get_basic_db_stats(),
      scenario_results: scenario_results,
      configuration: %{
        concurrency: concurrency,
        iterations: iterations,
        scenarios: @scenarios
      }
    }

    print_performance_report(results)
    results
  end

  # Helper function to deterministically choose a scenario based on execution ID
  defp choose_scenario(execution_id) do
    hash = :erlang.phash2(execution_id)

    case rem(hash, 4) do
      0 -> :untouched
      1 -> :basic_info
      2 -> :with_ssn
      3 -> :nearly_complete
    end
  end

  # Basic database statistics without complex telemetry
  defp get_basic_db_stats() do
    # Get basic statistics from database
    executions_count = from(e in Journey.Persistence.Schema.Execution, select: count(e.id)) |> Journey.Repo.one()
    values_count = from(v in Journey.Persistence.Schema.Execution.Value, select: count(v.id)) |> Journey.Repo.one()

    computations_count =
      from(c in Journey.Persistence.Schema.Execution.Computation, select: count(c.id)) |> Journey.Repo.one()

    %{
      total_executions: executions_count,
      total_values: values_count,
      total_computations: computations_count,
      note: "Complex telemetry disabled for compatibility"
    }
  end

  # Scenario 0: Sweeper "test"
  defp run_scenario(:sweeper, _concurrency, _iterations) do
    # Trigger multiple background sweeps to stress scheduler queries
    sweep_results =
      1..4
      |> Enum.map(fn i ->
        Process.sleep(1000)

        IO.puts("[#{i}] running abandoned sweeps")
        abandoned_sweep_start = System.monotonic_time(:millisecond)
        Journey.Scheduler.Background.Sweeps.Abandoned.sweep(nil)
        abandoned_sweep_duration = System.monotonic_time(:millisecond) - abandoned_sweep_start
        IO.puts("[#{i}] abandoned sweeps completed after #{abandoned_sweep_duration}ms")

        IO.puts("[#{i}] running ScheduleNodes sweeps")
        background_sweep_start = System.monotonic_time(:millisecond)
        {_kicked_count, _sweep_run_id} = Journey.Scheduler.Background.Sweeps.ScheduleNodes.sweep(nil)
        background_sweep_duration = System.monotonic_time(:millisecond) - background_sweep_start
        IO.puts("[#{i}] ScheduleNodes sweeps completed after #{background_sweep_duration}ms")

        IO.puts("[#{i}] running unblocked sweeps")
        unblocked_sweep_start = System.monotonic_time(:millisecond)
        Journey.Scheduler.BackgroundSweeps.UnblockedBySchedule.sweep(nil, 5)
        unblocked_sweep_duration = System.monotonic_time(:millisecond) - unblocked_sweep_start
        IO.puts("[#{i}] unblocked sweeps completed after #{unblocked_sweep_duration}ms")

        {i, abandoned_sweep_duration, background_sweep_duration, unblocked_sweep_duration}
      end)

    %{
      executions_created: 0,
      background_sweeps: sweep_results |> length(),
      sweeps_timing:
        "\n" <>
          (sweep_results
           |> Enum.map(fn {i, abandoned_sweep_duration, background_sweep_duration, unblocked_sweep_duration} ->
             """
             #{i}:
                 abandoned sweep: #{abandoned_sweep_duration}
                 background sweep: #{background_sweep_duration}
                 unblocked sweeps: #{unblocked_sweep_duration}
                 total: #{abandoned_sweep_duration + background_sweep_duration + unblocked_sweep_duration} ms
             """
           end)
           |> Enum.join(""))
    }
  end

  # Scenario 1: Scheduler Stress Test
  defp run_scenario(:scheduler_stress, concurrency, iterations) do
    # Create many executions with various computation states
    executions =
      1..iterations
      |> Enum.map(fn _ ->
        graph = Journey.Examples.CreditCardApplication.graph()
        Journey.start_execution(graph)
      end)

    # Set different values to create mixed computation states
    tasks =
      executions
      |> Enum.chunk_every(div(length(executions), concurrency) + 1)
      |> Enum.map(fn chunk ->
        Task.async(fn ->
          Enum.each(chunk, fn execution ->
            # Partially complete some executions to create various states
            scenario = choose_scenario(execution.id)

            case scenario do
              :untouched ->
                # Leave in initial state
                :ok

              :basic_info ->
                # Set some inputs
                execution
                |> Journey.set_value(:full_name, "Test User #{scenario}")
                |> Journey.set_value(:birth_date, "01/01/1990")

              :with_ssn ->
                # Complete more steps
                execution
                |> Journey.set_value(:full_name, "Test User #{scenario}")
                |> Journey.set_value(:birth_date, "01/01/1990")
                |> Journey.set_value(:ssn, "111-22-3333")

              :nearly_complete ->
                # Nearly complete
                execution
                |> Journey.set_value(:full_name, "Test User #{scenario}")
                |> Journey.set_value(:birth_date, "01/01/1990")
                |> Journey.set_value(:ssn, "111-22-3333")
                |> Journey.set_value(:email_address, "test@example.com")
            end
          end)
        end)
      end)

    Task.await_many(tasks, 60_000)

    # Trigger multiple background sweeps to stress scheduler queries
    sweep_tasks =
      1..5
      |> Enum.map(fn _ ->
        Task.async(fn ->
          [
            elem(Journey.Scheduler.Background.Sweeps.ScheduleNodes.sweep(nil), 0),
            Journey.Scheduler.Background.Sweeps.UnblockedBySchedule.sweep(nil, 5)
          ]
        end)
      end)

    sweep_results = Task.await_many(sweep_tasks, 30_000)
    total_sweeps = sweep_results |> List.flatten() |> length()

    %{
      executions_created: length(executions),
      background_sweeps: total_sweeps
    }
  end

  # Scenario 2: High-Frequency Value Updates
  defp run_scenario(:high_frequency_values, concurrency, iterations) do
    # Create base executions
    # Reduced for faster testing
    executions =
      1..div(iterations, 4)
      |> Enum.map(fn _ ->
        graph = Journey.Examples.CreditCardApplication.graph()
        Journey.start_execution(graph)
      end)

    # Rapidly update values across executions
    update_tasks =
      1..concurrency
      |> Enum.map(fn worker_id ->
        Task.async(fn ->
          # Reduced updates per worker
          1..div(iterations, 2)
          |> Enum.each(fn i ->
            execution = Enum.at(executions, rem(i, length(executions)))

            # Rapidly update different node values
            case rem(i, 4) do
              0 -> Journey.set_value(execution, :full_name, "Updated #{worker_id}-#{i}")
              1 -> Journey.set_value(execution, :birth_date, "#{rem(i, 12) + 1}/#{rem(i, 28) + 1}/#{1990 + rem(i, 30)}")
              2 -> Journey.set_value(execution, :email_address, "user#{worker_id}.#{i}@example.com")
              3 -> Journey.set_value(execution, :ssn, "#{100 + rem(i, 899)}-#{10 + rem(i, 89)}-#{1000 + rem(i, 8999)}")
            end
          end)
        end)
      end)

    Task.await_many(update_tasks, 60_000)

    %{
      base_executions: length(executions),
      total_updates: concurrency * div(iterations, 2)
    }
  end

  # Scenario 3: Time-Based Queries
  defp run_scenario(:time_based_queries, concurrency, iterations) do
    # Create executions with staggered timing
    executions =
      1..div(iterations, 2)
      |> Enum.map(fn i ->
        graph = Journey.Examples.CreditCardApplication.graph()
        execution = Journey.start_execution(graph)

        # Stagger the value setting times slightly
        if rem(i, 5) == 0, do: Process.sleep(1)
        execution |> Journey.set_value(:full_name, "Time Test #{i}")

        execution
      end)

    # Query for recent changes using time-based filters
    # Reduced queries
    query_tasks =
      1..div(concurrency, 2)
      |> Enum.map(fn _ ->
        Task.async(fn ->
          now = System.system_time(:second)
          # Last 30 seconds
          cutoff_time = now - 30

          # Query for recent value changes
          from(v in Journey.Persistence.Schema.Execution.Value,
            where: v.set_time >= ^cutoff_time,
            select: count(v.id)
          )
          |> Journey.Repo.one()
        end)
      end)

    Task.await_many(query_tasks, 30_000)

    %{
      executions_with_staggered_times: length(executions),
      time_based_queries: div(concurrency, 2)
    }
  end

  # Scenario 4: Archive/Unarchive Workload
  defp run_scenario(:archive_unarchive_workload, _concurrency, iterations) do
    # Create executions
    # Reduced for faster testing
    executions =
      1..div(iterations, 2)
      |> Enum.map(fn _ ->
        graph = Journey.Examples.CreditCardApplication.graph()
        Journey.start_execution(graph)
      end)

    # Archive half of them
    executions
    |> Enum.take(div(length(executions), 2))
    |> Enum.each(fn execution ->
      Journey.Executions.archive_execution(execution.id)
    end)

    # Query active executions (should use partial index)
    # Just a few queries
    active_counts =
      1..5
      |> Enum.map(fn _ ->
        from(e in Journey.Persistence.Schema.Execution,
          where: is_nil(e.archived_at),
          select: count(e.id)
        )
        |> Journey.Repo.one()
      end)

    %{
      total_executions: length(executions),
      archived_count: div(length(executions), 2),
      active_queries: 5,
      sample_active_count: List.first(active_counts)
    }
  end

  # Scenario 5: Scheduled Computation Load
  defp run_scenario(:scheduled_computation_load, _concurrency, iterations) do
    # Create executions and trigger scheduled computations
    # Reduced for faster testing
    executions =
      1..div(iterations, 4)
      |> Enum.map(fn _ ->
        graph = Journey.Examples.CreditCardApplication.graph()
        execution = Journey.start_execution(graph)

        # Complete enough steps to trigger scheduled computations
        execution
        |> Journey.set_value(:full_name, "Scheduled Test")
        |> Journey.set_value(:birth_date, "01/01/1990")
        |> Journey.set_value(:ssn, "111-22-3333")
        |> Journey.set_value(:email_address, "scheduled@example.com")
      end)

    # Let computations process briefly
    Process.sleep(2000)

    # Query scheduled computations (should use partial index)
    scheduled_count =
      from(c in Journey.Persistence.Schema.Execution.Computation,
        where: not is_nil(c.scheduled_time),
        select: count(c.id)
      )
      |> Journey.Repo.one()

    %{
      executions_with_schedules: length(executions),
      scheduled_computations_found: scheduled_count
    }
  end

  # Utility Functions
  defp get_memory_usage_mb() do
    total_memory = :erlang.memory(:total)
    total_memory / (1024 * 1024)
  end

  defp print_performance_report(results) do
    IO.puts("\n" <> String.duplicate("=", 80))
    IO.puts("JOURNEY PERFORMANCE BENCHMARK RESULTS")
    IO.puts(String.duplicate("=", 80))

    IO.puts("\nOVERALL METRICS:")
    IO.puts("  Total Duration: #{results.total_duration_ms} ms")

    IO.puts(
      "  Memory Usage: #{Float.round(results.memory.initial_mb, 2)} → #{Float.round(results.memory.final_mb, 2)} MB (Δ #{Float.round(results.memory.delta_mb, 2)} MB)"
    )

    IO.puts(
      "  Configuration: #{results.configuration.concurrency} concurrent, #{results.configuration.iterations} iterations"
    )

    IO.puts("\nDATABASE METRICS:")
    db = results.database_metrics
    IO.puts("  Total Executions: #{db.total_executions}")
    IO.puts("  Total Values: #{db.total_values}")
    IO.puts("  Total Computations: #{db.total_computations}")
    IO.puts("  Note: #{db.note}")

    IO.puts("\nSCENARIO RESULTS:")

    Enum.each(results.scenario_results, fn {scenario, metrics} ->
      IO.puts("  #{scenario}:")
      IO.puts("    Duration: #{metrics.duration_ms} ms")

      metrics
      |> Map.delete(:duration_ms)
      |> Enum.each(fn {key, value} ->
        IO.puts("    #{key}: #{value}")
      end)
    end)

    IO.puts("\n" <> String.duplicate("=", 80))
  end
end

# Run the benchmark with default parameters
LoadTest.PerformanceBenchmark.run()

_still_computing_computations = """

journey_dev=# select computations.id, computations.state, computations.execution_id, computations.deadline - EXTRACT(EPOCH FROM NOW()), executions.archived_at from computations join executions on computations.execution_id = executions.id where state='computing' order by deadline desc;
           id            |   state   |       execution_id       | ?column?  | archived_at
-------------------------+-----------+--------------------------+-----------+-------------
 CMP1HM55EAXLYV485ZJ1EJZ | computing | EXEC5A6Z01BM8YZZXDXMBYJE | -3.495476 |
 CMP14AZGR5H1DR6TT7LB1EB | computing | EXEC8HHMJ8V65GDA852EBHHB | -3.495476 |
 CMPZ0XE0B7G7B7HDRX8XZTH | computing | EXEC00H927E2HE98R20T597B | -3.495476 |
 CMPB62MLZLZDYT8ETA64BYB | computing | EXEC2YMJEAJ86MRX6DD9VJA1 | -3.495476 |
 CMPA0J2ZYZDVZ64R8559HAJ | computing | EXECVJVX6TB7YG6DV0XTMHL9 | -3.495476 |
 CMPYM212H25VB4Z299ADH27 | computing | EXECJ6VEL0276EBVG6RJ2GHT | -3.495476 |
 CMPL063902VJM92RDYYJHTD | computing | EXECR31YR4JY13DXHDJ00YRR | -3.495476 |
 CMPL4Z1RJEZ3ZEE9RM42R7M | computing | EXECV4XBYZE3MXJMM0V1D16T | -3.495476 |
 CMPMAG9R86HAX2TJ6Y0E781 | computing | EXECA78EZ90MJ1MVL204YAAD | -3.495476 |
 CMPRYZ9ZZ88VA8ZD2A7AB8Y | computing | EXEC6THJJ70LZ06YA24A990M | -3.495476 |
 CMP4LJZB6TD8E1DRA846LRA | computing | EXEC2XT86G6AEG0TAGTG6EG5 | -3.495476 |
 CMPZR5G3LLJX3L0ZDJ7JGR3 | computing | EXECDGV5MJBVBJ6VEET447L6 | -3.495476 |
 CMPE9L4E7R80VYLHVER9735 | computing | EXECZL318YRLH55HD6379MVJ | -3.495476 |
 CMPBJ80EELLAAZB3T03GH29 | computing | EXECVRG764T4VTJTZB5RR603 | -3.495476 |
 CMPEBT821JV4Z3J4ED18D6B | computing | EXECDRD9046Y37ZX339656XJ | -3.495476 |
(15 rows)

"""
