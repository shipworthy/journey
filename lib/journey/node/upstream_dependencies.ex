defmodule Journey.Node.UpstreamDependencies do
  @moduledoc false

  require Logger

  def provided?(value_node), do: value_node.set_time != nil

  # def unblocked_when(list_of_required_upstream_nodes)
  #     when is_list(list_of_required_upstream_nodes) do

  # end

  def unblocked_when({upstream_node_name, f_condition} = r)
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    r
  end

  def unblocked_when({operation, left, right})
      when operation in [:and, :or] do
    {operation, unblocked_when(left), unblocked_when(right)}
  end

  def unblocked_when({:not, expr}) do
    {:not, unblocked_when(expr)}
  end

  def unblocked_when(invalid) do
    raise ArgumentError, """
    Invalid unblocked_when expression: #{inspect(invalid)}.
    Expected one of:

      - {node_name :: atom, condition_fun :: (any -> boolean)}
      - {:and | :or, left, right}
      - {:not, condition}
    """
  end

  def unblocked_when(upstream_node_name, f_condition) do
    unblocked_when({upstream_node_name, f_condition})
  end
end

defmodule Journey.Node.UpstreamDependencies.Computations do
  @moduledoc false
  def list_all_node_names({_operation, left, right}) do
    list_all_node_names(left) ++ list_all_node_names(right)
  end

  def list_all_node_names({upstream_node_name, _f_condition})
      when is_atom(upstream_node_name) do
    [upstream_node_name]
  end

  def evaluate_computation_for_readiness(
        all_executions_values,
        {:or, left, right}
      ) do
    result1 = evaluate_computation_for_readiness(all_executions_values, left)
    result2 = evaluate_computation_for_readiness(all_executions_values, right)

    if result1.ready? or result2.ready? do
      %{
        ready?: true,
        conditions_met: result1.conditions_met ++ result2.conditions_met,
        conditions_not_met: result1.conditions_not_met ++ result2.conditions_not_met
      }
    else
      %{
        ready?: false,
        conditions_met: result1.conditions_met ++ result2.conditions_met,
        conditions_not_met: result1.conditions_not_met ++ result2.conditions_not_met
      }
    end
  end

  def evaluate_computation_for_readiness(all_executions_values, {:and, left, right}) do
    result1 = evaluate_computation_for_readiness(all_executions_values, left)
    result2 = evaluate_computation_for_readiness(all_executions_values, right)

    if result1.ready? and result2.ready? do
      %{
        ready?: true,
        conditions_met: result1.conditions_met ++ result2.conditions_met,
        conditions_not_met: result1.conditions_not_met ++ result2.conditions_not_met
      }
    else
      %{
        ready?: false,
        conditions_met: result1.conditions_met ++ result2.conditions_met,
        conditions_not_met: result1.conditions_not_met ++ result2.conditions_not_met
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
