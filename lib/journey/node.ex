defmodule Journey.Node do
  @moduledoc """
  This module contains functions for creating nodes in a graph.
  Nodes in a graph can be of several types:
  * `input/1` – a node that takes input from the user.
  * `compute/4` – a node that computes a value based on its upstream nodes.
  * `mutate/4` – a node that mutates the value of another node.
  * `schedule_once/3` – a node that, once unblocked, in its turn, unblocks others, on a schedule.
  * `schedule_recurring/3` – a node that, once unblocked, in its turn, unblocks others, on a schedule, time after time.
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
  iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
  %{first_name: "Mario", execution_id: "...", last_updated_at: 1_234_567_890}
  ```

  """
  def input(name) when is_atom(name) do
    %Graph.Input{name: name}
  end

  @doc """
  Creates a self-computing node.

  `name` is an atom uniquely identifying the node in this graph.

  `gated_by` defines when this node becomes eligible to compute.
    Accepts either:
    - A list of atom node names, e.g. `[:a, :b]`, indicating the node becomes unblocked when all of the listed nodes have a value.
    - A structured condition (see [unblocked_when/1](`Journey.Node.UpstreamDependencies.unblocked_when/1`) )
      allowing for logical operators (`:and`, `:or`) and custom value predicates (e.g. `unblocked_when({:and, [{:a, &provided?/1}, {:b, &provided?/1}]})`).

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
  ...>             # This is useful for notifying other systems (e.g. a LiveView
  ...>             # via PubSub.notify()) – that the value has been saved.
  ...>             :ok
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution() |> Journey.set_value(:name, "Alice")
  iex> execution |> Journey.get_value(:pig_latin_ish_name, wait_any: true)
  {:ok, "Alice-ay"}
  iex> execution |> Journey.values() |> redact([:execution_id, :last_updated_at])
  %{name: "Alice", pig_latin_ish_name: "Alice-ay", execution_id: "...", last_updated_at: 1_234_567_890}
  ```

  """
  def compute(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :compute,
      gated_by: gated_by,
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
  iex> execution |> Journey.get_value(:remove_pii, wait_any: true)
  {:ok, "updated :name"}
  iex> execution |> Journey.values() |> redact([:execution_id, :last_updated_at])
  %{name: "redacted", remove_pii: "updated :name",  execution_id: "...", last_updated_at: 1_234_567_890}
  ```

  """
  def mutate(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :mutate,
      gated_by: gated_by,
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      mutates: Keyword.fetch!(opts, :mutates),
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
  ...>       "`archive()` doctest graph (a useless machine that archives itself immediately;)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         archive(:archive, [:name])
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution.archived_at == nil
  true
  iex> execution = Journey.set_value(execution, :name, "Mario")
  iex> {:ok, _} = Journey.get_value(execution, :archive, wait_any: true)
  iex> Journey.load(execution)
  nil
  iex> execution = Journey.load(execution, include_archived: true)
  iex> execution.archived_at == nil
  false
  ```

  """
  def archive(name, gated_by, opts \\ [])
      when is_atom(name) do
    %Graph.Step{
      name: name,
      type: :compute,
      gated_by: gated_by,
      f_compute: &archive_graph/1,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
    }
  end

  defp archive_graph(e) do
    archived_at = Journey.Executions.archive_execution(e.execution_id)
    {:ok, archived_at}
  end

  @doc """
  Creates a graph node that declares its readiness at a specific time, once.

  Once this node is unblocked, it will be executed to set the time at which it will unblock its downstream dependencies.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`schedule_once()` doctest graph (it reminds you to take a nap in a couple of seconds;)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         schedule_once(
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
  iex> # This is only needed in a test, to simulate the background processing that happens in non-tests automatically.
  iex> background_sweeps_task = Journey.Scheduler.BackgroundSweeps.start_background_sweeps_in_test(execution.id)
  iex> execution |> Journey.get_value(:nap_time, wait_any: 10_000) # this will take a couple of seconds.
  {:ok, "It's time to take a nap, Mario!"}
  iex> Journey.Scheduler.BackgroundSweeps.stop_background_sweeps_in_test(background_sweeps_task)

  ```

  """
  def schedule_once(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :schedule_once,
      gated_by: gated_by,
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
    }
  end

  @doc """
  Creates a graph node that declares its readiness at a specific time, time after time.

  Once this node is unblocked, it will be repeatedly computed, to set the time at which it will unblock its downstream dependencies.

  This is useful for triggering recurring tasks, such as sending reminders or notifications.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`schedule_recurring()` doctest graph (it issues 'reminders' every few seconds)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         schedule_recurring(
  ...>           :schedule_a_reminder,
  ...>           [:name],
  ...>           fn _ ->
  ...>             soon = System.system_time(:second) + 2
  ...>             {:ok, soon}
  ...>           end
  ...>         ),
  ...>         compute(
  ...>           :send_a_reminder,
  ...>           [:name, :schedule_a_reminder],
  ...>           fn %{name: name} = v ->
  ...>             reminder_count = Map.get(v, :send_a_reminder, 0) + 1
  ...>             IO.puts("[\#{System.system_time(:second)}] \#{name}, here is your scheduled reminder # \#{reminder_count}.")
  ...>             {:ok, reminder_count}
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
  iex> # This is only needed in a test, to simulate the background processing that happens in non-tests automatically.
  iex> background_sweeps_task = Journey.Scheduler.BackgroundSweeps.start_background_sweeps_in_test(execution.id)
  iex> # Wait for initial reminders
  iex> {:ok, count1} = Journey.get_value(execution, :send_a_reminder, wait_any: 5_000)
  iex> count1 >= 1
  true
  iex> # Reload and wait for more reminders to verify recurring behavior
  iex> execution = Journey.load(execution)
  iex> {:ok, count2} = Journey.get_value(execution, :send_a_reminder, wait_new: 5_000)
  iex> count2 > count1
  true
  iex> Journey.Scheduler.BackgroundSweeps.stop_background_sweeps_in_test(background_sweeps_task)

  ```

  """

  def schedule_recurring(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    %Graph.Step{
      name: name,
      type: :schedule_recurring,
      gated_by: gated_by,
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60)
      #      grace_window_seconds: Keyword.get(opts, :grace_window_seconds, 60)
    }
  end

  @doc false
  def redact(map, key) when is_map(map) and is_atom(key) do
    map
    |> Map.update!(
      key,
      fn
        {:set, value} when is_binary(value) -> {:set, "..."}
        {:set, value} when is_integer(value) -> {:set, 1_234_567_890}
        value when is_binary(value) -> "..."
        value when is_integer(value) -> 1_234_567_890
      end
    )
  end

  @doc false
  def redact(map, keys) when is_map(map) and is_list(keys) do
    keys
    |> Enum.reduce(
      map,
      fn key, acc when is_atom(key) and is_map(acc) ->
        redact(acc, key)
      end
    )
  end
end
