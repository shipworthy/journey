defmodule Journey.Node do
  @moduledoc """
  This module contains functions for creating nodes in a graph.
  Nodes in a graph can be of several types:
  * `input/1` – a node that takes input from the user.
  * `compute/4` – a node that computes a value based on its upstream nodes.
  * `mutate/4` – a node that mutates the value of another node.
  * `pulse_once/3` – a node that, once unblocked, in its turn, unblocks others, on a schedule.
  * `pulse_recurring/3` – a node that, once unblocked, in its turn, unblocks others, on a schedule, time after time.
  """

  alias Journey.Graph

  @doc """
  Creates a graph input node. The value of an input node is set with `Journey.set_value/3`. The name of the node must be an atom.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`input()` doctest graph (just a few input nodes)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:first_name),
  ...>         input(:last_name),
  ...>         input(:zip_code)
  ...>        ]
  ...>     )
  iex> execution = graph |> Journey.start_execution() |> Journey.set_value(:first_name, "Mario")
  iex> Journey.values(execution)
  %{first_name: "Mario"}
  ```

  """
  def input(name) when is_atom(name) do
    %Graph.Input{name: name}
  end

  @doc """
  Creates a self-computing node.

  The name must be an atom.

  `upstream_nodes` is a list of atoms identifying the nodes that must have values before the computation executes.

  `f_compute` is the function that computes the value of the node, once the upstream dependencies are satisfied.
  The function is provided a map of the upstream nodes and their values as its argument and returns a tuple:
   - `{:ok, value}` or
   - `{:error, reason}`.
  The function is called when the upstream nodes are set, and the value is set to the result of the function.

  In the case of a failure, the function is automatically retried, up to `max_retries` times.
  If the function fails after `max_retries` attempts, the node is marked as failed.
  If the function does not return within `abandon_after_seconds`, it is considered abandoned, and it will be retried (up to `max_retries` times).

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`compute()` doctest graph (pig-latinize-ish a name)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         compute(
  ...>           :pig_latin_ish_name,
  ...>           [:name],
  ...>           fn %{name: name} ->
  ...>             {:ok, "\#{name}-ay"}
  ...>           end,
  ...>           max_retries: 4, # Optional (default: 3)
  ...>           abandon_after_seconds: 60, # Optional (default: 60)
  ...>           f_on_save: fn _execution_id, _params ->
  ...>             # Optional callback to be called when the value is saved.
  ...>             # This is useful for notifying other systems (e.g. a LiveView via PubSub.notify() – that the value has been saved).
  ...>             :ok
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution() |> Journey.set_value(:name, "Alice")
  iex> execution |> Journey.get_value(:pig_latin_ish_name, wait: true)
  {:ok, "Alice-ay"}
  iex> execution |> Journey.values()
  %{name: "Alice", pig_latin_ish_name: "Alice-ay"}
  ```

  """
  def compute(name, upstream_nodes, f_compute, opts \\ [])
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :compute,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
    }
  end

  @doc """
  Creates a graph node that mutates the value of another node.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`mutate()` doctest graph (a useless machine;)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         mutate(
  ...>           :remove_pii,
  ...>           [:name],
  ...>           fn %{name: _name} ->
  ...>             # Return the new value for the "name" node.
  ...>             {:ok, "redacted"}
  ...>           end,
  ...>           mutates: :name # The name of an existing node whose value will be mutated.
  ...>         )
  ...>       ]
  ...>     )
  iex> execution =
  ...>     graph
  ...>     |> Journey.start_execution()
  ...>     |> Journey.set_value(:name, "Mario")
  iex> execution |> Journey.get_value(:remove_pii, wait: true)
  {:ok, "updated :name"}
  iex> execution |> Journey.values()
  %{name: "redacted", remove_pii: "updated :name"}
  ```

  """
  def mutate(name, upstream_nodes, f_compute, opts \\ [])
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :compute,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      mutates: Keyword.fetch!(opts, :mutates),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
    }
  end

  @doc """
  Creates a graph node that declares its readiness at a specific time.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`pulse_once()` doctest graph (it reminds you to take a nap in a couple of seconds;)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         pulse_once(
  ...>           :schedule_a_nap,
  ...>           [:name],
  ...>           fn %{name: _name} ->
  ...>             # This function is to return the time (in epoch seconds) at which
  ...>             # its downstream dependencies should be unblocked.
  ...>             in_two_seconds = System.system_time(:second) + 2
  ...>             {:ok, in_two_seconds}
  ...>           end
  ...>         ),
  ...>         compute(
  ...>           :nap_time,
  ...>           [:name, :schedule_a_nap],
  ...>           fn %{name: name, schedule_a_nap: _time_to_take_a_nap} ->
  ...>             {:ok, "It's time to take a nap, \#{name}!"}
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution =
  ...>     graph
  ...>     |> Journey.start_execution()
  ...>     |> Journey.set_value(:name, "Mario")
  iex> execution |> Journey.values() |> Map.get(:name)
  "Mario"
  iex> # This is only needed in a test, to simulate what automatically happens in non-tests.
  iex> Task.start(fn ->
  ...>   for _ <- 1..3 do
  ...>     :timer.sleep(2_000)
  ...>     Journey.Scheduler.BackgroundSweep.find_and_kick_recently_due_pulse_values(execution.id)
  ...>   end
  ...> end)
  iex> execution |> Journey.get_value(:nap_time, wait: 10_000) # this will take a couple of seconds.
  {:ok, "It's time to take a nap, Mario!"}
  ```

  """
  def pulse_once(name, upstream_nodes, f_compute, opts \\ [])
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :pulse_once,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
    }
  end

  @doc """
  TODO: Document this function.

  """
  def pulse_recurring(name, upstream_nodes, f_compute, opts \\ [])
      when is_atom(name) and is_list(upstream_nodes) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :pulse_recurring,
      upstream_nodes: upstream_nodes,
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
    }
  end
end
