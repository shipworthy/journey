defmodule Journey.Node.Conditions do
  @doc """
  This is a helper function provided for use in `gated_by` conditions. This function checks if the supplied node has a value (ands, for schedule nodes (`schedule_once`, `schedule_recurring`), that its time has come).
  """
  def provided?(%{node_type: node_type} = value_node) when node_type in [:schedule_once, :schedule_recurring] do
    now = System.system_time(:second)
    due_in_seconds = if value_node.node_value == nil, do: nil, else: value_node.node_value - now
    value_node.set_time != nil and due_in_seconds != nil and due_in_seconds <= 0
  end

  def provided?(value_node), do: value_node.set_time != nil

  def true?(value_node), do: value_node.set_time != nil and value_node.node_value == true
  def false?(value_node), do: value_node.set_time != nil and value_node.node_value == false
end
