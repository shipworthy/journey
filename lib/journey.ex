defmodule Journey do
  @moduledoc """
  Documentation for `Journey`.
  """

  alias Journey.Execution
  alias Journey.Executions
  alias Journey.Graph

  def new_graph(name, version, nodes)
      when is_binary(name) and is_binary(version) and is_list(nodes) do
    Graph.new(name, version, nodes)
    |> Journey.Graph.validate()
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

  def compute(name, upstream_nodes, f_compute, opts \\ [])
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :compute,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute,
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
    }
  end

  def mutate(name, upstream_nodes, f_compute, opts \\ [])
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :compute,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute,
      mutates: Keyword.fetch!(opts, :mutates),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
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

  def start_execution(graph) when is_struct(graph, Graph) do
    Journey.Executions.create_new(
      graph.name,
      graph.version,
      graph.nodes
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

  @doc """
  Returns the value of a node in an execution. Optionally waits for the value to be set.

  # Options:
  * `:wait` – whether or not to wait for the value to be set. This option can have the following vales:
    * `false` or `0` – do not wait, return the current value. This is the default.
    * `true` – wait until the value is available, or until timeout
    * a positive integer – wait for the supplied number of milliseconds (default: 30_000)
    * `:infinity` – wait indefinitely

  Returns
  * `{:ok, value}` if the value is set,
  * `{:error, :not_set}` if the value is not set, and
  * `{:error, :no_such_value}` if the node does not exist.

  ## Examples

      iex> execution =
      ...>    Journey.Examples.Horoscope.graph() |>
      ...>    Journey.start_execution() |>
      ...>    Journey.set_value(:birth_day, 26)
      iex> Journey.get_value(execution, :birth_day)
      {:ok, 26}
      iex> Journey.get_value(execution, :birth_month)
      {:error, :not_set}
      iex> Journey.get_value(execution, :astrological_sign)
      {:error, :not_set}
      iex> execution = Journey.set_value(execution, :birth_month, "April")
      iex> Journey.get_value(execution, :astrological_sign)
      {:error, :not_set}
      iex> Journey.get_value(execution, :astrological_sign, wait: true)
      {:ok, "Taurus"}
      iex> Journey.get_value(execution, :horoscope, wait: 2_000)
      {:error, :not_set}
      iex> execution = Journey.set_value(execution, :first_name, "Mario")
      iex> Journey.get_value(execution, :horoscope, wait: true)
      {:ok, "🍪s await, Taurus Mario!"}


  """

  def get_value(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    default_timeout_ms = 5_000

    timeout_ms =
      opts
      |> Keyword.get(:wait, false)
      |> case do
        false ->
          nil

        0 ->
          nil

        true ->
          default_timeout_ms

        :infinity ->
          :infinity

        ms when is_integer(ms) and ms > 0 ->
          ms
      end

    Executions.get_value(execution, node_name, timeout_ms)
  end
end
