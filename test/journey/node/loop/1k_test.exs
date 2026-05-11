defmodule Journey.Node.Loop.OneKTest do
  # async: false intentionally — this test runs a 1000-iteration loop and
  # logs per-iteration timing. Running it concurrently with other tests would
  # contend for scheduler/DB resources and make the timing measurement noisy
  # and non-comparable across runs.
  use ExUnit.Case, async: false

  import Journey.Helpers.Random, only: [random_string: 0]
  import Journey.Node

  @iterations 1_000
  @timeout_ms 120_000

  test "1000-iteration self-feeding loop completes and reports per-iteration timing" do
    graph =
      Journey.new_graph(
        "loop_1k_#{random_string()}",
        "v1",
        [
          input(:trigger),
          loop(
            :counter,
            [:trigger],
            fn values ->
              n = values[:counter] || 0
              {:cont_with_fallback, n + 1}
            end,
            max_iterations: @iterations
          ),
          compute(:done, [:counter], fn %{counter: n} -> {:ok, n} end)
        ]
      )

    execution = Journey.start_execution(graph)

    t0 = System.monotonic_time(:millisecond)
    execution = Journey.set(execution, :trigger, true)

    {:ok, terminal_value, _rev} =
      Journey.get(execution, :done, wait: :any, timeout: @timeout_ms)

    elapsed_ms = System.monotonic_time(:millisecond) - t0

    assert terminal_value == @iterations

    per_iter_ms = elapsed_ms / @iterations
    iter_per_sec = @iterations / (elapsed_ms / 1000)

    IO.puts(
      "\nloop throughput: #{@iterations} iterations in #{elapsed_ms} ms " <>
        "(#{Float.round(per_iter_ms, 2)} ms/iter, #{Float.round(iter_per_sec, 1)} iter/s)"
    )
  end
end
