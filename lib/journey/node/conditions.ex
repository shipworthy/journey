defmodule Journey.Node.Conditions do
  @moduledoc """
  This module contains helper functions for use when defining upstream dependencies for `compute` modules.
  """

  @doc """
  This is a helper function provided for use in `unblocked_when` conditions.
  This function checks if the supplied node has a value.
  For "scheduled" types of nodes (`schedule_once`, `schedule_recurring`) it also checks that the scheduled time has come).

  ## Examples

  ```elixir
  iex> import Journey.Node
  iex> import Journey.Node.Conditions
  iex> import Journey.Node.UpstreamDependencies
  iex> graph = Journey.new_graph(
  ...>   "greeting workflow, doctest for provided?",
  ...>   "v1.0.0",
  ...>   [
  ...>     input(:name),
  ...>     compute(
  ...>       :greeting,
  ...>       unblocked_when(:name, &provided?/1),
  ...>       fn %{name: name} -> {:ok, "Hello, \#{name}!"} end
  ...>     )
  ...>   ]
  ...> )
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set(execution, :name, "Alice")
  iex> Journey.get_value(execution, :greeting, wait: :any)
  {:ok, "Hello, Alice!"}
  ```
  """
  def provided?(%{node_type: node_type} = value_node) when node_type in [:schedule_once, :schedule_recurring] do
    now = System.system_time(:second)
    due_in_seconds = if value_node.node_value == nil, do: nil, else: value_node.node_value - now
    value_node.set_time != nil and due_in_seconds != nil and due_in_seconds <= 0
  end

  def provided?(value_node), do: value_node.set_time != nil

  @doc """
  This is a helper function provided for use in `unblocked_when` conditions.
  This function checks if the upstream node's value is `true`.

  ## Examples

  ```elixir
  iex> import Journey.Node
  iex> import Journey.Node.Conditions
  iex> import Journey.Node.UpstreamDependencies
  iex> graph = Journey.new_graph(
  ...>   "umbrella forecast graph, doctest for true?",
  ...>   "v1.0.0",
  ...>   [
  ...>     input(:it_will_rain_tomorrow),
  ...>     compute(
  ...>       :umbrella,
  ...>       unblocked_when(:it_will_rain_tomorrow, &true?/1),
  ...>       fn %{it_will_rain_tomorrow: true} -> {:ok, "need to pack my umbrella"} end
  ...>     )
  ...>   ]
  ...> )
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set(execution, :it_will_rain_tomorrow, true)
  iex> Journey.get_value(execution, :umbrella, wait: :any)
  {:ok, "need to pack my umbrella"}
  iex> execution = Journey.set(execution, :it_will_rain_tomorrow, false)
  iex> Journey.get_value(execution, :umbrella)
  {:error, :not_set}

  ```
  """
  def true?(value_node), do: value_node.set_time != nil and value_node.node_value == true

  @doc """
  This is a helper function provided for use in `unblocked_when` conditions.
  This function checks if the upstream node's value is `false`.

  ## Examples

  ```elixir
  iex> import Journey.Node
  iex> import Journey.Node.Conditions
  iex> import Journey.Node.UpstreamDependencies
  iex> graph = Journey.new_graph(
  ...>   "umbrella forecast graph, doctest for false?",
  ...>   "v1.0.0",
  ...>   [
  ...>     input(:it_will_rain_tomorrow),
  ...>     compute(
  ...>       :todays_preparation,
  ...>       unblocked_when(:it_will_rain_tomorrow, &false?/1),
  ...>       fn %{it_will_rain_tomorrow: false} -> {:ok, "prepare my bike"} end
  ...>     )
  ...>   ]
  ...> )
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set(execution, :it_will_rain_tomorrow, false)
  iex> Journey.get_value(execution, :todays_preparation, wait: :any)
  {:ok, "prepare my bike"}
  iex> execution = Journey.set(execution, :it_will_rain_tomorrow, true)
  iex> Journey.get_value(execution, :todays_preparation)
  {:error, :not_set}

  ```
  """
  def false?(value_node), do: value_node.set_time != nil and value_node.node_value == false
end
