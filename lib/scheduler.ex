defmodule Journey.Scheduler do
  @moduledoc false

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph

  require Logger
  import Journey.Helpers.Log

  def advance(execution) do
    available_computations = grab_available_computations(execution)

    if length(available_computations) > 0 do
      execution = Journey.load(execution)

      available_computations
      |> Enum.each(fn to_compute -> schedule_computation(execution, to_compute) end)

      # TODO: do a bit of load / performance testing to see if we get any benefit from advancing
      # the execution here. (Until then, since we have just examined all candidates for computation, this seems unlikely
      # to provide any benefit.)
      # advance(execution)
      Journey.load(execution)
    else
      execution
    end
  end

  defp schedule_computation(execution, computation) do
    # Here we would update the execution with the scheduled computation
    # For example, we could set the start time and state of the computation
    # execution = %{execution | scheduled_computation: computation}

    graph_node =
      execution.graph_name
      |> Journey.Graph.Catalog.fetch!()
      |> Journey.Graph.find_node_by_name(computation.node_name)

    computation_params = Journey.values_available(execution)

    Task.start(fn ->
      Logger.info("[#{execution.id}][#{computation.node_name}] [#{mf()}] starting async computation")

      computation_result = graph_node.f_compute.(computation_params)

      Logger.info(
        "[#{execution.id}][#{computation.node_name}] [#{mf()}] async computation completed with result: #{inspect(computation_result)}"
      )

      # TODO: consider killing the computation after deadline (since we are likely to
      # start other instances of the computation, doing this sounds like a good idea).
      # This could probably be as simple as some version of starting the computation as linked to this process,
      # and exiting the parent after the deadline or when the computation completes, whichever comes first.
      #
      # t = Task.async(fn ->  node.f_compute.(params)  end)
      # Task.await(t, abandoned_after)

      # TODO: add retries
      # computation_result = {:ok, 12}
      # computation_result = {:error, "oh noooooooooo"}

      Logger.info("[#{execution.id}][#{computation.node_name}] [#{mf()}] completed async computation.")
      mark_computation_as_completed(computation, computation_result)
      advance(execution)
    end)

    execution
  end

  defp mark_computation_as_completed(computation, {:ok, result}) do
    prefix = "[#{computation.execution_id}][#{computation.node_name}] [#{mf()}]"
    Logger.info("#{prefix}: marking as completed. starting.")

    node_name_as_string = computation.node_name |> Atom.to_string()

    {:ok, _} =
      Journey.Repo.transaction(fn repo ->
        Logger.info("#{prefix}: marking as completed. transaction starting.")

        # Increment revision on the execution, for updating the value.
        {1, [new_revision]} =
          from(e in Execution,
            update: [inc: [revision: 1]],
            where: e.id == ^computation.execution_id,
            select: e.revision
          )
          |> repo.update_all([])

        # Record result / value.
        from(v in Value,
          where: v.execution_id == ^computation.execution_id and v.node_name == ^node_name_as_string
        )
        |> repo.update_all(
          set: [node_value: %{"v" => result}, set_time: System.system_time(:second), ex_revision: new_revision]
        )

        # Mark the computation as "completed".
        computation
        |> Ecto.Changeset.change(%{
          completion_time: System.system_time(:second),
          state: :success,
          ex_revision_at_completion: new_revision
        })
        |> repo.update!()

        Logger.info("#{prefix}: marking as completed. transaction done.")
      end)

    Logger.info("#{prefix}: marking as completed. done.")
  end

  defp mark_computation_as_completed(computation, {:error, error_details}) do
    prefix = "[#{computation.execution_id}][#{computation.node_name}] [#{mf()} :error]"
    Logger.info("#{prefix}: marking as completed. starting.")

    {:ok, _} =
      Journey.Repo.transaction(fn repo ->
        Logger.info("#{prefix}: marking as completed. transaction starting.")

        # Increment revision on the execution, for updating the value.
        {1, [new_revision]} =
          from(e in Execution,
            update: [inc: [revision: 1]],
            where: e.id == ^computation.execution_id,
            select: e.revision
          )
          |> repo.update_all([])

        # Mark the computation as "failed".
        computation
        |> Ecto.Changeset.change(%{
          error_details: "#{inspect(error_details)}",
          completion_time: System.system_time(:second),
          state: :failed,
          ex_revision_at_completion: new_revision
        })
        |> repo.update!()

        Logger.info("#{prefix}: marking as completed. transaction done.")
      end)

    Logger.info("#{prefix}: marking as completed. done.")
  end

  defp from_computations(nil) do
    from(c in Computation)
  end

  defp from_computations(execution_id) do
    from(c in Computation,
      where: c.execution_id == ^execution_id
    )
  end

  def sweep_abandoned_computations(execution_id) do
    prefix = "[#{execution_id}] [#{mf()}]"
    Logger.info("#{prefix}: starting")

    current_epoch_second = System.system_time(:second)

    {:ok, abandoned_computations} =
      Journey.Repo.transaction(fn repo ->
        {count, list_of_abandoned_computations} =
          from(c in from_computations(execution_id),
            where: c.state == ^:computing and not is_nil(c.deadline) and c.deadline < ^current_epoch_second,
            select: c
          )
          |> repo.update_all(
            set: [
              state: :abandoned,
              completion_time: System.system_time(:second)
            ]
          )

        Logger.info("#{prefix}: found #{count} abandoned computation(s)")

        list_of_abandoned_computations
      end)

    abandoned_computations
    |> Journey.Executions.convert_values_to_atoms(:node_name)
    |> Enum.map(fn ac ->
      Logger.warning("#{prefix}: an abandoned computation, id: #{ac.id}, #{ac.execution_id}.#{ac.node_name}")
      ac
    end)
  end

  defp grab_available_computations(execution) when is_struct(execution, Execution) do
    Logger.info("[#{execution.id}] [#{mf()}] grabbing available computation")

    current_epoch_second = System.system_time(:second)
    graph = Journey.Graph.Catalog.fetch!(execution.graph_name)

    {:ok, computations_to_perform} =
      Journey.Repo.transaction(fn repo ->
        all_candidates_for_computation =
          from(c in Computation,
            # TODO: for abandoned computations, account for max retries.
            # (c.state == ^:not_set or
            #    (not is_nil(c.deadline) and c.state == ^:computing and c.deadline > ^current_epoch_second)) and
            where:
              c.execution_id == ^execution.id and
                (c.state == ^:not_set or
                   (not is_nil(c.deadline) and c.state == ^:computing and c.deadline < ^current_epoch_second)) and
                c.computation_type == ^:compute,
            # TODO: would no "SKIP LOCKED" be a better option?
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()

        all_set_values =
          from(v in Value,
            where: v.execution_id == ^execution.id and not is_nil(v.set_time),
            select: v.node_name
          )
          |> repo.all()
          |> Enum.map(fn node_name -> String.to_atom(node_name) end)
          |> MapSet.new()

        all_candidates_for_computation
        |> Journey.Executions.convert_values_to_atoms(:node_name)
        |> Enum.filter(fn computation -> upstream_dependencies_fulfilled?(graph, computation, all_set_values) end)
        |> Enum.map(fn unblocked_computation ->
          # Increment revision on the execution.
          {1, [new_revision]} =
            from(e in Execution, update: [inc: [revision: 1]], where: e.id == ^execution.id, select: e.revision)
            |> repo.update_all([])

          # Mark the computation as "computing".
          graph_node = Graph.find_node_by_name(graph, unblocked_computation.node_name)

          unblocked_computation
          |> Ecto.Changeset.change(%{
            state: :computing,
            start_time: System.system_time(:second),
            ex_revision_at_start: new_revision,
            deadline: System.system_time(:second) + graph_node.abandon_after_seconds
          })
          |> repo.update!()
        end)
      end)

    selected_computation_names =
      computations_to_perform
      |> Enum.map_join(", ", fn computation -> computation.node_name end)
      |> String.trim()

    Logger.info("[#{execution.id}] [#{mf()}] selected these computations: [#{selected_computation_names}]")

    computations_to_perform
  end

  defp upstream_dependencies_fulfilled?(graph, computation, all_set_values) do
    graph
    |> Graph.find_node_by_name(computation.node_name)
    |> Map.get(:upstream_nodes)
    |> Enum.all?(fn upstream_node_name ->
      MapSet.member?(all_set_values, upstream_node_name)
    end)
  end
end
