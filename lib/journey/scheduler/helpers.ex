defmodule Journey.Scheduler.Helpers do
  @moduledoc false

  import Ecto.Query

  require Logger

  alias Journey.Persistence.Schema.Execution

  def graph_from_execution_id(execution_id) do
    execution =
      execution_id
      |> Journey.Executions.load(false, true)

    Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
  end

  def graph_node_from_execution_id(execution_id, node_name) do
    execution_id
    |> graph_from_execution_id()
    |> Journey.Graph.find_node_by_name(node_name)
  end

  def increment_execution_revision_in_transaction(execution_id, repo) do
    if !repo.in_transaction?() do
      raise "must be called inside a transaction"
    end

    {1, [new_revision]} =
      from(e in Execution,
        update: [
          inc: [revision: 1],
          set: [updated_at: ^System.os_time(:second)]
        ],
        where: e.id == ^execution_id,
        select: e.revision
      )
      |> repo.update_all([])

    new_revision
  end

  @doc """
  Executes a transaction with automatic retry on deadlock detection.

  Uses exponential backoff with jitter to reduce contention:
  - Attempt 1: immediate
  - Attempt 2: 500ms + random(0-500ms)
  - Attempt 3: 1000ms + random(0-1000ms)
  - Attempt 4: 2000ms + random(0-2000ms)

  Returns the transaction result or error after max_retries exhausted.
  """
  def transaction_with_deadlock_retry(operation, prefix, max_retries \\ 3) do
    do_transaction_with_retry(operation, prefix, 0, max_retries)
  end

  defp do_transaction_with_retry(operation, prefix, attempt, max_retries) do
    if attempt > 0 do
      Logger.info("#{prefix}: [RETRY] Starting transaction attempt #{attempt + 1}/#{max_retries + 1}")
    end

    try do
      case Journey.Repo.transaction(operation) do
        {:ok, result} ->
          if attempt > 0 do
            Logger.info("#{prefix}: [RETRY] Transaction succeeded on attempt #{attempt + 1}/#{max_retries + 1}")
          end

          {:ok, result}

        {:error, error} ->
          {:error, error}
      end
    rescue
      error in Postgrex.Error ->
        case error.postgres do
          %{code: :deadlock_detected} ->
            if attempt < max_retries do
              delay_ms = calculate_backoff_with_jitter(attempt)

              Logger.info(
                "[RETRY] Deadlock detected (attempt #{attempt + 1}/#{max_retries + 1}), " <>
                  "retrying after #{delay_ms}ms. Details: #{inspect(error.postgres)}"
              )

              Process.sleep(delay_ms)
              do_transaction_with_retry(operation, prefix, attempt + 1, max_retries)
            else
              Logger.warning(
                "[RETRY] Deadlock detected after #{max_retries} retries, giving up. " <>
                  "Will rely on background sweeper. Details: #{inspect(error.postgres)}"
              )

              {:error, error}
            end

          _ ->
            Logger.info(
              "[RETRY] Non-deadlock Postgrex error raised. " <>
                "Code: #{inspect(error.postgres[:code])}, " <>
                "Full structure: #{inspect(error)}"
            )

            reraise error, __STACKTRACE__
        end
    end
  end

  defp calculate_backoff_with_jitter(attempt) do
    # Exponential backoff: 500ms * 2^attempt
    base_delay = (500 * :math.pow(2, attempt)) |> trunc()
    # Add random jitter from 0 to base_delay
    jitter = :rand.uniform(base_delay)
    base_delay + jitter
  end
end
