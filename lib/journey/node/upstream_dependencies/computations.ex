defmodule Journey.Node.UpstreamDependencies.Computations do
  @moduledoc false

  import Journey.Node.Conditions

  def list_all_node_names(node_names) when is_list(node_names) do
    node_names
  end

  def list_all_node_names({:not, {node_name, condition}}) when is_atom(node_name) and is_function(condition, 1) do
    [node_name]
  end

  def list_all_node_names({operation, conditions}) when operation in [:and, :or] and is_list(conditions) do
    conditions
    |> Enum.flat_map(fn c -> list_all_node_names(c) end)
  end

  def list_all_node_names({upstream_node_name, _f_condition})
      when is_atom(upstream_node_name) do
    [upstream_node_name]
  end

  def unblocked?(all_executions_values, gated_by, mode) when is_list(all_executions_values) do
    r = evaluate_computation_for_readiness(all_executions_values, gated_by, mode)
    r.ready?
  end

  # For invalidation purposes: a node is considered "provided" if it has been set,
  # regardless of its value. This preserves downstream values when schedule nodes
  # are paused (value=0) rather than invalidating them.
  defp provided_for_invalidation?(value_node), do: value_node.set_time != nil

  def upstream_nodes_and_functions(condition_spec, mode \\ :computation)

  def upstream_nodes_and_functions(node_names, mode) when is_list(node_names) do
    condition_fn = if mode == :invalidation, do: &provided_for_invalidation?/1, else: &provided?/1
    upstream_nodes_and_functions({:and, Enum.map(node_names, fn name -> {name, condition_fn} end)}, mode)
  end

  def upstream_nodes_and_functions({:not, {node_name, f_condition}}, _mode)
      when is_atom(node_name) and is_function(f_condition, 1) do
    [{node_name, f_condition}]
  end

  def upstream_nodes_and_functions({operation, conditions}, mode)
      when operation in [:and, :or] and is_list(conditions) do
    conditions
    |> Enum.flat_map(fn c -> upstream_nodes_and_functions(c, mode) end)
  end

  def upstream_nodes_and_functions({upstream_node_name, f_condition}, _mode)
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    [{upstream_node_name, f_condition}]
  end

  def evaluate_computation_for_readiness(all_executions_values, conditions, mode \\ :computation)

  def evaluate_computation_for_readiness(all_executions_values, list_of_required_node_names, mode)
      when is_list(list_of_required_node_names) do
    conditions =
      list_of_required_node_names
      |> Journey.Node.UpstreamDependencies.unblocked_when()

    evaluate_computation_for_readiness(all_executions_values, conditions, mode)
  end

  def evaluate_computation_for_readiness(all_executions_values, {:or, conditions}, mode) when is_list(conditions) do
    results =
      conditions
      |> Enum.map(fn c -> evaluate_computation_for_readiness(all_executions_values, c, mode) end)

    any_met = Enum.any?(results, fn r -> r.ready? end)

    %{
      ready?: any_met,
      conditions_met: Enum.flat_map(results, fn r -> r.conditions_met end),
      conditions_not_met: Enum.flat_map(results, fn r -> r.conditions_not_met end),
      structure: %{
        type: :or,
        met?: any_met,
        children: Enum.map(results, fn r -> r.structure end)
      }
    }
  end

  def evaluate_computation_for_readiness(all_executions_values, {:and, conditions}, mode) when is_list(conditions) do
    results =
      conditions
      |> Enum.map(fn c -> evaluate_computation_for_readiness(all_executions_values, c, mode) end)

    all_met = Enum.all?(results, fn c -> c.ready? end)

    %{
      ready?: all_met,
      conditions_met: Enum.flat_map(results, fn r -> r.conditions_met end),
      conditions_not_met: Enum.flat_map(results, fn r -> r.conditions_not_met end),
      structure: %{
        type: :and,
        met?: all_met,
        children: Enum.map(results, fn r -> r.structure end)
      }
    }
  end

  def evaluate_computation_for_readiness(all_executions_values, {upstream_node_name, f_condition}, mode)
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    relevant_value_node =
      all_executions_values
      |> Enum.find(fn %{node_name: node_name} -> node_name == upstream_node_name end)

    if relevant_value_node == nil do
      raise "missing value node for #{upstream_node_name}"
    end

    # In invalidation mode, use relaxed criteria for schedule nodes only
    # (just check if set_time != nil, regardless of value)
    # For all other nodes, use the actual condition function
    condition_met =
      if mode == :invalidation and
           relevant_value_node.node_type in [
             :schedule_once,
             :tick_once,
             :schedule_recurring,
             :tick_recurring
           ] do
        provided_for_invalidation?(relevant_value_node)
      else
        f_condition.(relevant_value_node)
      end

    condition_data = %{upstream_node: relevant_value_node, f_condition: f_condition, condition_context: :direct}

    %{
      ready?: condition_met,
      conditions_met: if(condition_met, do: [condition_data], else: []),
      conditions_not_met: if(condition_met, do: [], else: [condition_data]),
      structure: %{
        type: :leaf,
        met?: condition_met,
        condition: condition_data
      }
    }
  end

  def evaluate_computation_for_readiness(all_executions_values, {:not, {upstream_node_name, f_condition}}, mode)
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    relevant_value_node =
      all_executions_values
      |> Enum.find(fn %{node_name: node_name} -> node_name == upstream_node_name end)

    if relevant_value_node == nil do
      raise "missing value node for #{upstream_node_name}"
    end

    # In invalidation mode, use relaxed criteria for schedule nodes only
    # For all other nodes, use the actual condition function
    inner_condition_met =
      if mode == :invalidation and
           relevant_value_node.node_type in [
             :schedule_once,
             :tick_once,
             :schedule_recurring,
             :tick_recurring
           ] do
        provided_for_invalidation?(relevant_value_node)
      else
        f_condition.(relevant_value_node)
      end

    negated_condition_met = not inner_condition_met
    condition_data = %{upstream_node: relevant_value_node, f_condition: f_condition, condition_context: :negated}

    %{
      ready?: negated_condition_met,
      conditions_met: if(negated_condition_met, do: [condition_data], else: []),
      conditions_not_met: if(negated_condition_met, do: [], else: [condition_data]),
      structure: %{
        type: :not,
        met?: negated_condition_met,
        child: %{
          type: :leaf,
          met?: inner_condition_met,
          condition: %{upstream_node: relevant_value_node, f_condition: f_condition, condition_context: :direct}
        }
      }
    }
  end
end
