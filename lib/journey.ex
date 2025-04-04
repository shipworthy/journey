defmodule Journey do
  @moduledoc """
  Documentation for `Journey`.
  """

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

  def new_graph(name, inputs_and_steps) when is_binary(name) and is_list(inputs_and_steps) do
    Graph.new(name, inputs_and_steps)
    |> Graph.Catalog.register()
  end

  def load(execution_id) when is_binary(execution_id) do
    # JourneyExecution.load(execution_id)
  end

  def load(execution) when is_struct(execution, Execution) do
    # Execution.load(execution.id)
  end

  def input(name) when is_atom(name) do
    %Graph.Input{name: name}
  end

  def step(name, upstream_nodes, f_compute)
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute
    }
  end

  def start_graph_execution(graph) when is_struct(graph, Graph) do
    # Execution.new(graph)
  end

  def values(execution) when is_struct(execution, Execution) do
    # Execution.values(execution, opts)
  end

  def set_value(execution, node_name, value)
      when is_struct(execution, Execution) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value)) do
    # Logger.info("[#{execution.id}] [#{mf()}] '#{node_name}'")
    # Execution.set_value(execution, node_name, value)
  end

  def get_value(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    _backoff =
      Keyword.get(opts, :wait, false)
      |> case do
        false ->
          []

        true ->
          [1000, 1000, 1000, 1000, 1000, 1000]

        list_of_backoff_values when is_list(list_of_backoff_values) ->
          list_of_backoff_values
      end

    # if wait do
    #   wait_backoff = Keyword.get(opts, :wait_backoff, [1000, 1000, 1000, 1000, 1000, 1000])
    #   Execution.get_value_with_blocking_retries(execution, node_name, wait_backoff)
    # else
    #   Logger.info("[#{execution.id}] [#{mf()}] no wait: '#{node_name}'")
    #   Execution.get_value(execution, node_name)
    # end
  end
end
