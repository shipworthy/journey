defmodule Journey.Scheduler.Completions do
  @moduledoc false

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value

  require Logger
  import Journey.Helpers.Log

  def record_success(computation, inputs_to_capture, result) do
    {:ok, _} =
      Journey.Repo.transaction(fn repo ->
        record_success_in_transaction(repo, computation, inputs_to_capture, result)
      end)
  end

  def record_error(computation, error_details) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [#{mf()} :error]"
    Logger.info("#{prefix}: marking as completed. starting.")

    {:ok, _} =
      Journey.Repo.transaction(fn repo ->
        Logger.info("#{prefix}: marking as completed. transaction starting.")

        current_computation =
          from(c in Computation, where: c.id == ^computation.id)
          |> repo.one!()

        if current_computation.state == :computing do
          # Increment revision on the execution, for updating the value.
          {1, [new_revision]} =
            from(e in Execution,
              update: [inc: [revision: 1]],
              where: e.id == ^computation.execution_id,
              select: e.revision
            )
            |> repo.update_all([])

          # Mark the computation as "failed".
          # TODO: we might need to store inputs_to_capture in the computation, instead of the value.
          computation
          |> Ecto.Changeset.change(%{
            error_details: "#{inspect(error_details)}",
            completion_time: System.system_time(:second),
            state: :failed,
            ex_revision_at_completion: new_revision
          })
          |> repo.update!()
          |> Journey.Scheduler.Retry.maybe_schedule_a_retry(repo)

          Logger.info("#{prefix}: marking as completed. transaction done.")
        else
          Logger.warning(
            "#{prefix}: computation completed, but it is no longer :computing. (#{current_computation.state})"
          )
        end
      end)

    Logger.info("#{prefix}: marking as completed. done.")
  end

  defp record_success_in_transaction(repo, computation, inputs_to_capture, result) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [#{mf()}]"
    Logger.info("#{prefix}: starting.")

    graph_node = Journey.Scheduler.Helpers.graph_node_from_execution_id(computation.execution_id, computation.node_name)

    Logger.info("#{prefix}: marking as completed. transaction starting.")

    current_computation =
      from(c in Computation, where: c.id == ^computation.id)
      |> repo.one!()

    if current_computation.state == :computing do
      # Increment revision on the execution, for updating the value.
      {1, [new_revision]} =
        from(e in Execution,
          update: [inc: [revision: 1]],
          where: e.id == ^computation.execution_id,
          select: e.revision
        )
        |> repo.update_all([])

      # record_result(
      #   repo,
      #   graph_node.mutates,
      #   computation.node_name,
      #   computation.execution_id,
      #   new_revision,
      #   result
      # )

      computation.computation_type
      |> case do
        :compute ->
          record_result(
            repo,
            graph_node.mutates,
            computation.node_name,
            computation.execution_id,
            new_revision,
            result
          )

        :mutation ->
          record_result(
            repo,
            graph_node.mutates,
            computation.node_name,
            computation.execution_id,
            new_revision,
            result
          )

        :pulse_once ->
          record_result(
            repo,
            graph_node.mutates,
            computation.node_name,
            computation.execution_id,
            new_revision,
            result
          )

        :pulse_recurring ->
          record_result(
            repo,
            graph_node.mutates,
            computation.node_name,
            computation.execution_id,
            new_revision,
            result
          )
      end

      # Mark the computation as "completed".
      computation
      |> Ecto.Changeset.change(%{
        completion_time: System.system_time(:second),
        state: :success,
        computed_with: inputs_to_capture,
        ex_revision_at_completion: new_revision
      })
      |> repo.update!()

      # TODO: if the computation was triggered by a pulse_recurring computation, create a new pulse_recurring computation for a future event.

      Logger.info("#{prefix}: done. marking as completed.")
    else
      Logger.warning(
        "#{prefix}: done. computation completed, but it is no longer :computing. (#{current_computation.state})"
      )
    end
  end

  defp record_result(repo, node_to_mutate, node_name, execution_id, new_revision, result)
       when is_nil(node_to_mutate) do
    # Record the result in the corresponding value node.
    set_value(
      execution_id,
      node_name,
      new_revision,
      repo,
      result
    )
  end

  defp record_result(repo, node_to_mutate, node_name, execution_id, new_revision, result) do
    # Update this node to note that the mutation has been computed.
    set_value(
      execution_id,
      node_name,
      new_revision,
      repo,
      "updated #{inspect(node_to_mutate)}"
    )

    # Record the result in the value node being mutated.
    # Note that mutations are not regular updates, and do not trigger a recomputation,
    # so we don't update the value's revision.
    set_value(
      execution_id,
      node_to_mutate,
      nil,
      repo,
      result
    )
  end

  defp set_value(execution_id, node_name, new_revision, repo, value) do
    node_name_as_string = node_name |> Atom.to_string()

    from(v in Value,
      where: v.execution_id == ^execution_id and v.node_name == ^node_name_as_string
    )
    |> then(fn q ->
      if new_revision == nil do
        q
        |> repo.update_all(
          set: [
            node_value: %{"v" => value},
            set_time: System.system_time(:second)
          ]
        )
      else
        q
        |> repo.update_all(
          set: [
            node_value: %{"v" => value},
            set_time: System.system_time(:second),
            ex_revision: new_revision
          ]
        )
      end
    end)
  end
end
