defmodule Journey.Node.UpstreamDependencies do
  require Logger

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

  def unblocked_when({:not, {upstream_node_name, f_condition}} = r)
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    r
  end

  def unblocked_when(list_of_required_upstream_nodes)
      when is_list(list_of_required_upstream_nodes) do
    {:and, Enum.map(list_of_required_upstream_nodes, fn node_name -> {node_name, &provided?/1} end)}
  end

  @doc """
  """
  def unblocked_when({upstream_node_name, f_condition} = r)
      when is_atom(upstream_node_name) and is_function(f_condition, 1) do
    r
  end

  def unblocked_when({operation, conditions})
      when operation in [:and, :or] and is_list(conditions) do
    {operation, conditions}
  end

  def unblocked_when(invalid) do
    raise ArgumentError, """
    Invalid unblocked_when expression: #{inspect(invalid)}.
    """
  end

  @doc """
  This function is used to define the conditions under which a node is unblocked. It is intended to be used in the `gated_by` option of a node. The function takes a list of required upstream nodes and returns a predicate tree that can be used to check if the node is unblocked.

  The predicate tree can be a single node name, a function that takes a value node and returns a boolean, or a combination of these using `:and`, `:or`, and `:not` operations.

  The function also supports nested predicate trees, allowing for complex conditions to be defined.

  Examples:

  ```elixir
  iex> import Journey.Node
  iex> import Journey.Node.UpstreamDependencies
  iex> _graph =
  ...>   Journey.new_graph(
  ...>     "horoscope workflow - unblocked_when doctest",
  ...>     "v1.0.0",
  ...>     [
  ...>       input(:first_name),
  ...>       input(:birth_day),
  ...>       input(:birth_month),
  ...>       input(:suspended),
  ...>       compute(
  ...>         :zodiac_sign,
  ...>         # Computes itself once :birth_month and :birth_day have been provided:
  ...>         [:birth_month, :birth_day],
  ...>         fn %{birth_month: _birth_month, birth_day: _birth_day} ->
  ...>           # Everyone is a Taurus. ;)
  ...>           {:ok, "Taurus"}
  ...>         end
  ...>       ),
  ...>       compute(
  ...>         :horoscope,
  ...>         # Computes itself once :first_name and :zodiac_sign are in place, and if not suspended.
  ...>         unblocked_when({
  ...>           :and,
  ...>           [
  ...>             {:first_name, &provided?/1},
  ...>             {:zodiac_sign, &provided?/1},
  ...>             {:suspended, fn suspended -> suspended.node_value != true end}
  ...>           ]
  ...>         }),
  ...>         fn %{first_name: name, zodiac_sign: zodiac_sign} ->
  ...>           {:ok, "🍪s await, \#{zodiac_sign} \#{name}!"}
  ...>         end
  ...>       )
  ...>     ]
  ...>   )
  iex>
  ```

  """
  def unblocked_when(upstream_node_name, f_condition) do
    unblocked_when({upstream_node_name, f_condition})
  end
end
