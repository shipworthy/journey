defmodule Journey.Executions do
  alias Journey.Execution
  import Ecto.Query

  require Logger

  def create_new(graph_name, graph_version, inputs_and_steps, mutations) do
    {:ok, execution} =
      Journey.Repo.transaction(fn repo ->
        execution =
          %Execution{
            graph_name: graph_name,
            graph_version: graph_version,
            revision: 0
          }
          |> repo.insert!()

        # Create a value record for every graph node, regardless of the graph node's type.
        _values =
          inputs_and_steps
          |> Enum.map(fn
            graph_node ->
              %Execution.Value{
                execution: execution,
                node_name: Atom.to_string(graph_node.name),
                node_type: graph_node.type
                # ex_revision: execution.revision,
                # node_value: nil
              }
              |> repo.insert!()
          end)

        # Create computations for computable nodes.
        _computations =
          (inputs_and_steps ++ mutations)
          |> Enum.filter(fn %{type: type} -> type in Journey.Execution.ComputationType.values() end)
          |> Enum.map(fn computation ->
            %Execution.Computation{
              execution: execution,
              node_name: Atom.to_string(computation.name),
              computation_type: computation.type,
              # ex_revision_at_start: execution.revision,
              state: :not_set
            }
            |> repo.insert!()
            |> IO.inspect(label: :new_computation)
          end)

        # %Execution{
        #   execution
        #   | values: convert_values_to_atoms(values, :node_name),
        #     computations: convert_values_to_atoms(computations, :node_name)
        # }
        # TODO: investigate if this helps with loading newly updated data (making sure we always get it back), if not -- find a
        #  solution, and do this outside of the transaction.
        load(execution.id)
      end)

    execution
  end

  def load(execution_id) when is_binary(execution_id) do
    from(e in Execution, where: e.id == ^execution_id, preload: [:values, :computations])
    |> Journey.Repo.one!()
    |> convert_node_names_to_atoms()
  end

  def values(execution) do
    execution.values
    |> Enum.map(fn value ->
      node_status =
        if is_nil(value.set_time) do
          :not_set
        else
          {:set, if(is_nil(value.node_value), do: nil, else: Map.get(value.node_value, "v"))}
        end

      {value.node_name, node_status}
    end)
    |> Enum.into(%{})
  end

  def set_value(execution, node_name, value) do
    {:ok, updated_execution} =
      Journey.Repo.transaction(fn repo ->
        # Increment revision on the execution.
        {1, [new_revision]} =
          from(e in Execution, update: [inc: [revision: 1]], where: e.id == ^execution.id, select: e.revision)
          |> repo.update_all([])

        from(v in Execution.Value,
          where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
        )
        |> repo.update_all(
          # TODO: update revision on the value, and on the execution.
          set: [
            ex_revision: new_revision,
            node_value: %{"v" => value},
            set_time: System.system_time(:second)
          ]
        )

        # TODO: loading immediately after an update might be problematic – we might miss the last update,
        # depending on our db change propagation setup. tbd: does loading inside of a transaction help?
        load(execution.id)
      end)

    updated_execution
    |> Journey.Scheduler.advance()
  end

  def get_value(execution, node_name) do
    from(v in Execution.Value,
      where: v.execution_id == ^execution.id and v.node_name == ^Atom.to_string(node_name)
    )
    |> Journey.Repo.one()
    |> case do
      nil ->
        Logger.error("Value not found for node: #{node_name} in execution: #{execution.id}")
        nil

      %{set_time: nil} ->
        {:not_set, nil}

      %{node_value: node_value} ->
        {:set, node_value["v"]}
    end
  end

  defp convert_node_names_to_atoms(nil), do: nil

  defp convert_node_names_to_atoms(execution) do
    %Execution{
      execution
      | values: convert_values_to_atoms(execution.values, :node_name),
        computations: convert_values_to_atoms(execution.computations, :node_name)
    }
  end

  def convert_values_to_atoms(collection_of_maps, key) do
    collection_of_maps
    |> Enum.map(fn map ->
      Map.update!(map, key, &String.to_atom/1)
    end)
  end
end
