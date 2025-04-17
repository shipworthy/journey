defmodule Journey.Scheduler.Operations do
  @moduledoc false

  import Ecto.Query

  alias Journey.Execution
  alias Journey.Execution.Computation
  alias Journey.Execution.Value
  alias Journey.Graph

  require Logger
  import Journey.Helpers.Log

  def advance(execution) do
    prefix = "[#{execution.id}] [#{mf()}] [#{inspect(self())}]"
    Logger.info("#{prefix}: starting")
    graph = Journey.Graph.Catalog.fetch!(execution.graph_name)

    if graph == nil do
      Logger.error("#{prefix}: graph not found (#{inspect(execution.graph_name)})")
      execution
    else
      detect_updates_and_create_re_computations(execution, graph)

      available_computations = grab_available_computations(execution, graph)

      if length(available_computations) > 0 do
        execution = Journey.load(execution)

        available_computations
        |> Enum.each(fn to_compute -> launch_computation(execution, to_compute) end)

        Journey.load(execution)
      else
        execution
      end
      |> tap(fn _ -> Logger.info("#{prefix}: done") end)
    end
  end

  defp launch_computation(execution, computation) do
    computation_params = execution |> Journey.values(reload: false)

    Task.start(fn ->
      prefix = "[#{execution.id}.#{computation.node_name}.#{computation.id}] [#{mf()}] [#{execution.graph_name}]"
      Logger.info("#{prefix}: starting async computation")

      graph_node =
        execution.graph_name
        |> Journey.Graph.Catalog.fetch!()
        |> Journey.Graph.find_node_by_name(computation.node_name)

      input_versions_to_capture =
        execution
        |> Map.get(:values)
        |> Enum.map(fn v ->
          {
            v.node_name,
            v.ex_revision
          }
        end)
        |> Enum.into(%{})
        |> Map.take(graph_node.upstream_nodes)

      recurring_upstream_pulses_to_reschedule =
        find_things_to_reschedule(computation.computed_with, input_versions_to_capture)

      graph_node.f_compute.(computation_params)
      |> case do
        {:ok, _result} = computation_result ->
          Logger.info("#{prefix}: async computation completed successfully")
          mark_computation_as_completed(computation, input_versions_to_capture, computation_result)

        {:error, _error_details} = computation_result ->
          Logger.warning("#{prefix}: async computation completed with an error")
          mark_computation_as_completed(computation, input_versions_to_capture, computation_result)
          jitter_ms = :rand.uniform(10_000)
          Process.sleep(jitter_ms)
      end

      # TODO: how do we make sure rescheduling does not fall through cracks if something fails along the way?
      # TODO: perform the reschedule as part of the same transaction as marking the computation as completed.
      reschedule_recurring(recurring_upstream_pulses_to_reschedule)
      advance(execution)

      # TODO: consider killing the computation after deadline (since we are likely to
      # start other instances of the computation, doing this sounds like a good idea).
      # This could probably be as simple as some version of starting the computation as linked to this process,
      # and exiting the parent after the deadline or when the computation completes, whichever comes first.
      #
      # t = Task.async(fn ->  node.f_compute.(params)  end)
      # Task.await(t, abandoned_after)
      # pros: no lingering tasks. cons: abrupt termination, extra logic.
    end)

    execution
  end

  defp find_things_to_reschedule(original_computation_input_versions, new_computation_input_versions)
       when (original_computation_input_versions == nil or is_map(original_computation_input_versions)) and
              is_map(new_computation_input_versions) do
    # return the list of input nodes whose versions have changed, which are pulse_recurring
    []
  end

  defp reschedule_recurring(recurring_upstream_pulses_to_reschedule)
       when is_list(recurring_upstream_pulses_to_reschedule) do
    # create new computations for each of the pulse_recurring nodes identified in
    # recurring_upstream_pulses_to_reschedule
  end

  defp mark_computation_as_completed(computation, inputs_to_capture, {:ok, result}) do
    prefix = "[#{computation.execution_id}.#{computation.node_name}.#{computation.id}] [#{mf()}]"
    Logger.info("#{prefix}: marking as completed. starting.")

    graph_node = Journey.Scheduler.Helpers.graph_node_from_execution_id(computation.execution_id, computation.node_name)

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

          Logger.info("#{prefix}: marking as completed. transaction done.")
        else
          Logger.warning(
            "#{prefix}: computation completed, but it is no longer :computing. (#{current_computation.state})"
          )
        end
      end)

    Logger.info("#{prefix}: marking as completed. done.")
  end

  defp mark_computation_as_completed(computation, _inputs_to_capture, {:error, error_details}) do
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

  def an_upstream_node_has_a_newer_version?(computation, graph, all_computed_values) do
    upstream_nodes =
      graph
      |> Graph.find_node_by_name(computation.node_name)
      |> Map.get(:upstream_nodes)

    all_upstream_nodes_have_values? =
      upstream_nodes
      |> Enum.all?(fn upstream_node_name ->
        Map.has_key?(all_computed_values, upstream_node_name)
      end)

    at_least_one_upstream_node_has_a_higher_version? =
      upstream_nodes
      |> Enum.any?(fn upstream_node_name ->
        Map.has_key?(all_computed_values, upstream_node_name) and
          computation.ex_revision_at_start <= all_computed_values[upstream_node_name].node_revision
      end)

    all_upstream_nodes_have_values? and at_least_one_upstream_node_has_a_higher_version?
  end

  def detect_updates_and_create_re_computations(execution, graph) do
    prefix = "[#{execution.id}] [EXPERIMENT#{mf()}]"
    Logger.info("#{prefix}: starting")

    {:ok, new_computations} =
      Journey.Repo.transaction(fn repo ->
        latest_computation_ids =
          from(c in Computation,
            where: c.execution_id == ^execution.id and c.computation_type == :compute and c.state != ^:not_set,
            order_by: [desc: c.ex_revision_at_start],
            distinct: c.node_name,
            select: c.id
          )

        all_computations =
          from(c in Computation,
            where: c.id in subquery(latest_computation_ids),
            # TODO: here and elsewhere, experiment with a regular "SELECT FOR UPDATE", no "SKIP LOCKED".
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()
          |> Journey.Executions.convert_values_to_atoms(:node_name)

        all_set_values = get_all_set_values(execution.id, repo)

        all_computations
        |> Enum.filter(fn c -> an_upstream_node_has_a_newer_version?(c, graph, all_set_values) end)
        |> Enum.map(fn computation_to_re_create ->
          new_computation =
            %Execution.Computation{
              execution: execution,
              node_name: Atom.to_string(computation_to_re_create.node_name),
              computation_type: computation_to_re_create.computation_type,
              state: :not_set
            }
            |> repo.insert!()

          Logger.info(
            "#{prefix}: created a new re-computation, #{new_computation.id}.#{new_computation.node_name}. an upstream node has a newer version"
          )

          new_computation
        end)
      end)

    Logger.info("#{prefix}: completed. created #{length(new_computations)} new computations")
  end

  defp get_all_set_values(execution_id, repo) do
    now = System.system_time(:second)
    yesterday = now - 24 * 60 * 60

    from(v in Value,
      where:
        v.execution_id == ^execution_id and not is_nil(v.set_time) and
          (v.node_type == :compute or v.node_type == :input or
             (v.node_type == :pulse_once and fragment("CAST(?->>'v' AS INTEGER) < ?", v.node_value, ^now) and
                fragment("CAST(?->>'v' AS INTEGER) > ?", v.node_value, ^yesterday))),
      select: %{
        node_name: v.node_name,
        node_revision: v.ex_revision,
        node_value: v.node_value,
        set_time: v.set_time
      }
    )
    |> repo.all()
    |> Enum.map(fn %{node_name: node_name} = n ->
      node_name_as_atom = String.to_atom(node_name)
      {node_name_as_atom, %{n | node_name: String.to_atom(node_name)}}
    end)
    |> Enum.into(%{})
  end

  defp grab_available_computations(execution, nil) when is_struct(execution, Execution) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.error("#{prefix}: unknown graph #{execution.graph_name}")
    []
  end

  defp grab_available_computations(execution, graph)
       when is_struct(execution, Execution) and is_struct(graph, Journey.Graph) do
    prefix = "[#{execution.id}] [#{mf()}]"
    Logger.info("#{prefix}: grabbing available computation")

    {:ok, computations_to_perform} =
      Journey.Repo.transaction(fn repo ->
        all_candidates_for_computation =
          from(c in Computation,
            where:
              c.execution_id == ^execution.id and
                c.state == ^:not_set and
                c.computation_type in [^:compute, ^:pulse_once, ^:pulse_recurring],
            lock: "FOR UPDATE SKIP LOCKED"
          )
          |> repo.all()
          |> Journey.Executions.convert_values_to_atoms(:node_name)

        all_set_values =
          from(v in Value,
            where: v.execution_id == ^execution.id and not is_nil(v.set_time)
          )
          |> repo.all()
          |> Enum.filter(fn
            %{node_type: :compute} ->
              true

            %{node_type: :pulse_once, node_value: %{"v" => enabled_at}} ->
              System.system_time(:second) >= enabled_at

            %{node_type: :pulse_recurring, node_value: %{"v" => enabled_at}} ->
              System.system_time(:second) >= enabled_at

            %{node_type: :input} ->
              true
          end)
          |> Enum.map(fn %{node_name: node_name} = n -> %Value{n | node_name: String.to_atom(node_name)} end)
          |> Enum.map(fn %{node_name: node_name} -> node_name end)
          |> MapSet.new()

        all_candidates_for_computation
        |> Enum.filter(fn computation -> upstream_dependencies_fulfilled?(graph, computation, all_set_values) end)
        |> Enum.map(fn unblocked_computation ->
          grab_this_computation(graph, execution, unblocked_computation, repo)
        end)
      end)

    selected_computation_names =
      computations_to_perform
      |> Enum.map_join(", ", fn computation -> computation.node_name end)
      |> String.trim()

    Logger.info("#{prefix}: selected these computations: [#{selected_computation_names}]")

    computations_to_perform
  end

  defp grab_this_computation(graph, execution, computation, repo) do
    # Increment revision on the execution.
    {1, [new_revision]} =
      from(e in Execution, update: [inc: [revision: 1]], where: e.id == ^execution.id, select: e.revision)
      |> repo.update_all([])

    # Mark the computation as "computing".
    graph_node = Graph.find_node_by_name(graph, computation.node_name)

    computation
    |> Ecto.Changeset.change(%{
      state: :computing,
      start_time: System.system_time(:second),
      ex_revision_at_start: new_revision,
      deadline: System.system_time(:second) + graph_node.abandon_after_seconds
    })
    |> repo.update!()
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
