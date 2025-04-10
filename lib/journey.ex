defmodule Journey do
  @moduledoc """
  Documentation for `Journey`.
  """

  alias Journey.Execution
  alias Journey.Executions
  alias Journey.Graph

  @doc """
  Hello world.

  ## Examples

      iex> Journey.hello()
      :world

  """
  def hello do
    :world
  end

  def new_graph(name, version, inputs_and_steps, mutations)
      when is_binary(name) and is_binary(version) and is_list(inputs_and_steps) and is_list(mutations) do
    Graph.new(name, version, inputs_and_steps, mutations)
    |> Graph.Catalog.register()
  end

  def load(execution_id) when is_binary(execution_id) do
    Journey.Executions.load(execution_id)
  end

  def load(execution) when is_struct(execution, Execution) do
    load(execution.id)
  end

  # types of nodes and mutations.
  def input(name) when is_atom(name) do
    %Graph.Input{name: name}
  end

  def compute(name, upstream_nodes, f_compute, _opts \\ [])
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :compute,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute
    }
  end

  def pulse_recurring(name, upstream_nodes, f_compute)
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :pulse_recurring,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute
    }
  end

  def pulse_once(name, upstream_nodes, f_compute)
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :pulse_once,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute
    }
  end

  def mutate(name, upstream_nodes, f_compute)
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :mutation,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute
    }
  end

  def start_execution(graph) when is_struct(graph, Graph) do
    Journey.Executions.create_new(
      graph.name,
      graph.version,
      graph.inputs_and_steps,
      graph.mutations
    )
  end

  def values(execution) when is_struct(execution, Execution) do
    Executions.values(execution)
  end

  def values_available(execution) when is_struct(execution, Execution) do
    execution
    |> values()
    |> Enum.filter(fn {_k, v} ->
      v
      |> case do
        {:set, _} -> true
        _ -> false
      end
    end)
    |> Enum.map(fn {k, {:set, v}} -> {k, v} end)
    |> Enum.into(%{})
  end

  def set_value(execution, node_name, value)
      when is_struct(execution, Execution) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value)) do
    Journey.Executions.set_value(execution, node_name, value)
  end

  def wait(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    _backoff = backoff_strategy_from_opts(opts)
  end

  def get_value(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    _backoff = backoff_strategy_from_opts(opts)
    Executions.get_value(execution, node_name)

    # if wait do
    #   wait_backoff = Keyword.get(opts, :wait_backoff, [1000, 1000, 1000, 1000, 1000, 1000])
    #   Execution.get_value_with_blocking_retries(execution, node_name, wait_backoff)
    # else
    #   Logger.info("[#{execution.id}] [#{mf()}] no wait: '#{node_name}'")
    #   Execution.get_value(execution, node_name)
    # end
  end

  defp backoff_strategy_from_opts(opts) do
    Keyword.get(opts, :wait, false)
    |> case do
      false ->
        []

      true ->
        [1000, 1000, 1000, 1000, 1000, 1000]

      list_of_backoff_values when is_list(list_of_backoff_values) ->
        list_of_backoff_values
    end
  end
end
