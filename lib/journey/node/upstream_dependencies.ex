defmodule Journey.Node.UpstreamDependencies do
  @moduledoc false

  require Logger

  def provided?(%{node_type: node_type} = value_node) when node_type in [:schedule_once, :schedule_recurring] do
    now = System.system_time(:second)
    due_in_seconds = if value_node.node_value == nil, do: nil, else: value_node.node_value - now
    value_node.set_time != nil and due_in_seconds != nil and due_in_seconds <= 0
  end

  def provided?(value_node), do: value_node.set_time != nil

  def unblocked_when(list_of_required_upstream_nodes)
      when is_list(list_of_required_upstream_nodes) do
    {:and, Enum.map(list_of_required_upstream_nodes, fn node_name -> {node_name, &provided?/1} end)}
  end

  def unblocked_when({upstream_node_name, f_condition} = r)
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    r
  end

  def unblocked_when({operation, conditions})
      when operation in [:and, :or] and is_list(conditions) do
    {operation, conditions}
  end

  def unblocked_when({:not, expr}) do
    {:not, unblocked_when(expr)}
  end

  def unblocked_when(invalid) do
    raise ArgumentError, """
    Invalid unblocked_when expression: #{inspect(invalid)}.
    """
  end

  def unblocked_when(upstream_node_name, f_condition) do
    unblocked_when({upstream_node_name, f_condition})
  end
end

defmodule Journey.Node.UpstreamDependencies.Computations do
  @moduledoc false

  def list_all_node_names(node_names) when is_list(node_names) do
    node_names
  end

  def list_all_node_names({_operation, conditions}) when is_list(conditions) do
    conditions
    |> Enum.flat_map(fn c -> list_all_node_names(c) end)
  end

  def list_all_node_names({upstream_node_name, _f_condition})
      when is_atom(upstream_node_name) do
    [upstream_node_name]
  end

  def evaluate_computation_for_readiness(all_executions_values, list_of_required_node_names)
      when is_list(list_of_required_node_names) do
    conditions =
      list_of_required_node_names
      |> Journey.Node.UpstreamDependencies.unblocked_when()

    evaluate_computation_for_readiness(all_executions_values, conditions)
  end

  def evaluate_computation_for_readiness(all_executions_values, {:or, conditions}) when is_list(conditions) do
    results = Enum.map(conditions, fn c -> evaluate_computation_for_readiness(all_executions_values, c) end)
    # result1 = evaluate_computation_for_readiness(all_executions_values, left)
    # result2 = evaluate_computation_for_readiness(all_executions_values, right)

    if Enum.any?(results, fn r -> r.ready? end) do
      %{
        ready?: true,
        conditions_met: Enum.flat_map(results, fn r -> r.conditions_met end),
        conditions_not_met: Enum.flat_map(results, fn r -> r.conditions_not_met end)
      }
    else
      %{
        ready?: false,
        conditions_met: Enum.flat_map(results, fn r -> r.conditions_met end),
        conditions_not_met: Enum.flat_map(results, fn r -> r.conditions_not_met end)
      }
    end
  end

  def evaluate_computation_for_readiness(all_executions_values, {:and, conditions}) when is_list(conditions) do
    results = Enum.map(conditions, fn c -> evaluate_computation_for_readiness(all_executions_values, c) end)
    # result1 = evaluate_computation_for_readiness(all_executions_values, left)
    # result2 = evaluate_computation_for_readiness(all_executions_values, right)

    if Enum.all?(results, fn c -> c.ready? end) do
      %{
        ready?: true,
        conditions_met: Enum.flat_map(results, fn r -> r.conditions_met end),
        conditions_not_met: Enum.flat_map(results, fn r -> r.conditions_not_met end)
      }
    else
      %{
        ready?: false,
        conditions_met: Enum.flat_map(results, fn r -> r.conditions_met end),
        conditions_not_met: Enum.flat_map(results, fn r -> r.conditions_not_met end)
      }
    end
  end

  def evaluate_computation_for_readiness(all_executions_values, {upstream_node_name, f_condition})
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    relevant_value_node =
      all_executions_values |> Enum.find(fn %{node_name: node_name} -> node_name == upstream_node_name end)

    if relevant_value_node == nil do
      raise "missing value node for #{upstream_node_name}"
    end

    if f_condition.(relevant_value_node) do
      %{
        ready?: true,
        conditions_met: [%{upstream_node: relevant_value_node, f_condition: f_condition}],
        conditions_not_met: []
      }
    else
      %{
        ready?: false,
        conditions_met: [],
        conditions_not_met: [%{upstream_node: relevant_value_node, f_condition: f_condition}]
      }
    end
  end
end
