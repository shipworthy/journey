#!/usr/bin/env elixir

# Load Test Script for Journey
# Run with: mix run test_load/sunny_day.exs

total_executions = 1000
IO.puts("Running Sunny Day load test with #{total_executions} executions...")

total_executions
|> LoadTest.SunnyDay.run()
|> case do
  %{success: true} = results ->
    IO.puts("✅ Load test PASSED!")
    IO.inspect(results.metrics, label: "Final Metrics")

  %{success: false} = results ->
    IO.puts("❌ Load test FAILED!")
    IO.inspect(results.metrics, label: "Final Metrics")
    System.halt(1)

  error ->
    IO.puts("💥 Load test ERROR!")
    IO.inspect(error)
    System.halt(1)
end
