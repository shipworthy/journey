defmodule Journey.Persistence.InMemory do
  @moduledoc """
  An in-memory execution store using an Agent-based dictionary.

  This module provides a simple key-value store for Journey executions,
  where the key is the execution ID and the value is the execution struct.
  It follows the same patterns as Journey.Graph.Catalog.
  """

  use Agent
  alias Journey.Persistence.Schema.Execution

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc """
  Stores an execution in the in-memory dictionary.

  ## Parameters
  * `execution` - A Journey.Persistence.Schema.Execution struct

  ## Returns
  * The execution struct that was stored

  ## Examples

      iex> graph = Journey.new_graph("test", "1.0.0", [])
      iex> execution = Journey.start_execution(graph)
      iex> stored = Journey.Persistence.InMemory.store(execution)
      iex> stored.id == execution.id
      true
  """
  def store(%Execution{id: id} = execution) when is_binary(id) do
    Agent.update(__MODULE__, fn state -> Map.put(state, id, execution) end)
    execution
  end

  @doc """
  Retrieves an execution from the in-memory dictionary by ID.

  ## Parameters
  * `execution_id` - String ID of the execution to retrieve

  ## Returns
  * The execution struct if found, nil otherwise

  ## Examples

      iex> graph = Journey.new_graph("test", "1.0.0", [])
      iex> execution = Journey.start_execution(graph)
      iex> Journey.Persistence.InMemory.store(execution)
      iex> fetched = Journey.Persistence.InMemory.fetch(execution.id)
      iex> fetched.id == execution.id
      true
      iex> Journey.Persistence.InMemory.fetch("nonexistent")
      nil
  """
  def fetch(execution_id) when is_binary(execution_id) do
    Agent.get(__MODULE__, fn state -> Map.get(state, execution_id) end)
  end

  @doc """
  Removes an execution from the in-memory dictionary.

  ## Parameters
  * `execution_id` - String ID of the execution to remove

  ## Returns
  * `:ok`

  ## Examples

      iex> graph = Journey.new_graph("test", "1.0.0", [])
      iex> execution = Journey.start_execution(graph)
      iex> Journey.Persistence.InMemory.store(execution)
      iex> Journey.Persistence.InMemory.delete(execution.id)
      :ok
      iex> Journey.Persistence.InMemory.fetch(execution.id)
      nil
  """
  def delete(execution_id) when is_binary(execution_id) do
    Agent.update(__MODULE__, fn state -> Map.delete(state, execution_id) end)
    :ok
  end

  @doc """
  Returns all executions stored in the in-memory dictionary.

  ## Returns
  * List of execution structs

  ## Examples

      iex> graph = Journey.new_graph("test", "1.0.0", [])
      iex> execution1 = Journey.start_execution(graph)
      iex> execution2 = Journey.start_execution(graph)
      iex> Journey.Persistence.InMemory.store(execution1)
      iex> Journey.Persistence.InMemory.store(execution2)
      iex> all_executions = Journey.Persistence.InMemory.list()
      iex> length(all_executions)
      2
  """
  def list() do
    Agent.get(__MODULE__, fn state -> Map.values(state) end)
  end

  @doc """
  Clears all executions from the in-memory dictionary.

  This function is primarily useful for testing and cleanup.

  ## Returns
  * `:ok`

  ## Examples

      iex> graph = Journey.new_graph("test", "1.0.0", [])
      iex> execution = Journey.start_execution(graph)
      iex> Journey.Persistence.InMemory.store(execution)
      iex> Journey.Persistence.InMemory.clear()
      :ok
      iex> Journey.Persistence.InMemory.list()
      []
  """
  def clear() do
    Agent.update(__MODULE__, fn _state -> %{} end)
    :ok
  end
end
