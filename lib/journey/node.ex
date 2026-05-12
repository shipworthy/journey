defmodule Journey.Node do
  @moduledoc """
  This module contains functions for creating nodes in a graph.
  Nodes in a graph can be of several types:
  * `input/1` – a node that takes input from the user.
  * `compute/4` – a node that computes a value based on its upstream nodes.
  * `mutate/4` – a node that mutates the value of another node.
  * `historian/3` – a node that tracks the history of changes to another node.
  * `tick_once/3` – a node that, once unblocked, in its turn, unblocks others, on a schedule.
  * `tick_recurring/3` – a node that, once unblocked, in its turn, unblocks others, on a schedule, time after time.
  * `loop/4` – a node that iterates a step function durably, threading state across iterations until termination or an iteration cap.
  """

  alias Journey.Graph

  @common_step_options [
    :f_on_save,
    :max_retries,
    :abandon_after_seconds,
    :heartbeat_interval_seconds,
    :heartbeat_timeout_seconds,
    :keep_latest_completed_computations,
    :keep_oldest_completed_computations
  ]

  @input_options [:f_on_save]

  @loop_options @common_step_options ++ [:max_iterations]

  @doc """
  Creates a graph input node. The value of an input node is set with `Journey.set/3`. The name of the node must be an atom.

  Accepts an optional keyword list of options:

  * `:f_on_save` - Callback invoked after the input value is persisted via
    `Journey.set/3`. Only fires with `{:ok, value}` (inputs don't error).
    See [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`) for
    the shared contract.

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
  iex> execution = graph |> Journey.start() |> Journey.set(:first_name, "Mario")
  iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
  %{first_name: "Mario", execution_id: "...", last_updated_at: 1_234_567_890}
  ```

  With `f_on_save`:

      input(:comment, f_on_save: fn execution_id, node_name, {:ok, value} ->
        Phoenix.PubSub.broadcast(MyApp.PubSub, "execution:\#{execution_id}", {node_name, value})
        :ok
      end)

  """
  def input(name, opts \\ []) when is_atom(name) do
    check_options(opts, @input_options)
    %Graph.Input{name: name, f_on_save: Keyword.get(opts, :f_on_save)}
  end

  @doc """
  Creates a self-computing node.

  `name` is an atom uniquely identifying the node in this graph.

  `gated_by` defines when this node becomes eligible to compute.
    Accepts either:
    - A list of atom node names, e.g. `[:a, :b]`, indicating the node becomes unblocked when all of the listed nodes have a value.
    - A keyword list with conditions, e.g. `[a: fn node -> node.node_value > 10 end]`, for conditional dependencies.
    - A mixed list combining atoms and keyword conditions, e.g. `[:a, :b, c: fn node -> node.node_value > 5 end]`.
    - A structured condition (see [unblocked_when/1](`Journey.Node.UpstreamDependencies.unblocked_when/1`) )
      allowing for logical operators (`:and`, `:or`) and custom value predicates (e.g. `unblocked_when({:and, [{:a, &provided?/1}, {:b, &provided?/1}]})`).

  `f_compute` is the function that computes the value of the node, once the upstream dependencies are satisfied.
  The function can accept either one or two arguments:
   - **Arity 1**: `fn values_map -> ... end` - Receives a map of node names to their values, for all nodes whose value has been set or computed
   - **Arity 2**: `fn values_map, value_nodes_map -> ... end` - Additionally receives value node data from upstream nodes

  The function must return a tuple:
   - `{:ok, value}` or
   - `{:error, reason}`.

  The `value_nodes_map` (when using arity-2) contains detailed information for each upstream dependency, keyed by node name.
  Each entry is a map with the following fields:
   - `:node_value` - The current value of the node
   - `:metadata` - Metadata set via `Journey.set/3`
   - `:revision` - The revision number when this value was set
   - `:set_time` - Unix timestamp when the value was set

  This is useful for accessing contextual information like author IDs, timestamps, or revision tracking.
  The function is called when the upstream nodes are set, and the value is set to the result of the function.

  Note that return values will be JSON-serialized for storage. If the returned `value` or `reason` contains atoms (e.g., `{:ok, :pending}` or `{:ok, %{status: :active}}`), those atoms will be converted to strings when retrieved via `get_value/3`.

  In the case of a failure, the function is automatically retried, up to `max_retries` times (default: 3).
  If the function still fails after all retry attempts, the node's value remains unset.
  If the function does not return within `abandon_after_seconds` (default: 60), it is considered abandoned, and it will be retried (up to `max_retries` times).

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
  ...>           f_on_save: fn _execution_id, _node_name, _result ->
  ...>             # Optional callback invoked when the value is saved.
  ...>             # Useful for notifying other systems (e.g. a LiveView
  ...>             # via PubSub.notify()). The 2-arity form is deprecated.
  ...>             :ok
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start() |> Journey.set(:name, "Alice")
  iex> {:ok, "Alice-ay", 3} = execution |> Journey.get(:pig_latin_ish_name, wait: :any)
  iex> execution |> Journey.values() |> redact([:execution_id, :last_updated_at])
  %{name: "Alice", pig_latin_ish_name: "Alice-ay", execution_id: "...", last_updated_at: 1_234_567_890}
  ```

  ## Keyword List Syntax for Conditional Dependencies

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "threshold alert example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:temperature),
  ...>         # Using keyword list syntax for conditional dependency
  ...>         compute(
  ...>           :high_temp_alert,
  ...>           [temperature: fn node -> node.node_value > 30 end],
  ...>           fn %{temperature: temp} ->
  ...>             {:ok, "High temperature alert: \#{temp}°C"}
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start()
  iex> execution = Journey.set(execution, :temperature, 25)
  iex> Journey.get(execution, :high_temp_alert)
  {:error, :not_set}
  iex> execution = Journey.set(execution, :temperature, 35)
  iex> {:ok, "High temperature alert: 35°C", 4} = Journey.get(execution, :high_temp_alert, wait: :any)
  ```

  ## Using Value Node Data in Compute Functions

  Value node data can be accessed by defining an arity-2 compute function. This is useful for accessing
  contextual information like author IDs, timestamps, revisions, or data provenance from upstream nodes.

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "compute with value node data example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:title),
  ...>         compute(
  ...>           :title_with_author,
  ...>           [:title],
  ...>           # Arity-2 function receives value node data from dependencies
  ...>           fn %{title: title}, value_nodes_map ->
  ...>             author = get_in(value_nodes_map, [:title, :metadata, "author_id"]) || "unknown"
  ...>             {:ok, "\#{title} by \#{author}"}
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start()
  iex> execution = Journey.set(execution, :title, "Hello", metadata: %{"author_id" => "user123"})
  iex> {:ok, result, _} = Journey.get(execution, :title_with_author, wait: :any)
  iex> result
  "Hello by user123"
  ```

  ## Return Values
  The f_compute function must return `{:ok, value}` or `{:error, reason}`. Note that atoms
  in the returned `value` and `reason` will be converted to strings when persisted.

  ## Options

  - `:f_on_save` — see [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`).
    On success, `result` is `{:ok, value}` where `value` is what
    `f_compute` returned.

  ## Computation Retention

  By default, all completed computation records are kept in the database. For nodes that
  accumulate many computations over time (especially downstream of `tick_recurring` nodes),
  you can limit how many are retained:

      compute(:send_summary, deps, &send/1,
        keep_latest_completed_computations: 50
      )

  After each successful completion, this deletes older computations beyond the retention
  window, while always preserving the 10 oldest successful computations. Set to `:all` to
  explicitly keep everything, or `nil` (default) to inherit from the graph-level setting.

  Can also be set graph-wide via `Journey.new_graph/4`'s `:keep_latest_completed_computations` option.

  """
  def compute(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    check_options(opts, @common_step_options)
    validate_retention_options(opts)

    %Graph.Step{
      name: name,
      type: :compute,
      gated_by: normalize_gated_by(gated_by),
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60),
      heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
      heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240),
      keep_latest_completed_computations: Keyword.get(opts, :keep_latest_completed_computations),
      keep_oldest_completed_computations: Keyword.get(opts, :keep_oldest_completed_computations)
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
  ...>     |> Journey.start()
  ...>     |> Journey.set(:name, "Mario")
  iex> {:ok, "updated :name", 3} = execution |> Journey.get(:remove_pii, wait: :any)
  iex> execution |> Journey.values() |> redact([:execution_id, :last_updated_at])
  %{name: "redacted", remove_pii: "updated :name",  execution_id: "...", last_updated_at: 1_234_567_890}
  ```

  ## Options

  - `:update_revision_on_change` (default: `false`) - When `true`, updating the mutated node's value also increments its revision,
    triggering downstream nodes to recompute. When `false`, mutations update the value without triggering recomputation.
    Setting this to `true` for a node that mutates an upstream dependency will raise a validation error to prevent cycles.
  - `:f_on_save` — see [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`).
    On success, `result` is `{:ok, "updated :target"}`.

  ## External Polling Pattern

  A common use case is polling external data sources and triggering downstream recomputations:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "current market pricing graph",
  ...>       "v1.0.0",
  ...>       [
  ...>         tick_recurring(:poll_schedule, [], fn _ -> {:ok, System.system_time(:second) + 2} end),
  ...>         input(:cached_price),
  ...>         mutate(:fetch_current_price, [:poll_schedule],
  ...>           fn %{cached_price: cached_price} -> {:ok, (cached_price || 0) + 1} end,
  ...>           mutates: :cached_price,
  ...>           update_revision_on_change: true
  ...>         ),
  ...>         compute(:on_cached_price_update, [:cached_price],
  ...>           fn %{cached_price: new_price} -> {:ok, "price changed to \#{new_price}"} end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start() |> Journey.set(:cached_price, 5)
  iex> {:ok, "price changed to 5", initial_rev} = Journey.get(execution, :on_cached_price_update, wait: :any)
  iex> background_sweeps_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)
  iex> {:ok, "updated :cached_price", _} = Journey.get(execution, :fetch_current_price, wait: :any)
  iex> {:ok, "price changed to 6", _} = Journey.get(execution, :on_cached_price_update, wait: {:newer_than, initial_rev})
  iex> Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_sweeps_task)
  ```

  ## Return Values
  The f_compute function must return `{:ok, value}` or `{:error, reason}`. Note that atoms
  in the returned `value` and `reason` will be converted to strings when persisted.

  """
  def mutate(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    check_options(opts, @common_step_options ++ [:mutates, :update_revision_on_change])
    validate_retention_options(opts)

    %Graph.Step{
      name: name,
      type: :mutate,
      gated_by: normalize_gated_by(gated_by),
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      mutates: Keyword.fetch!(opts, :mutates),
      update_revision_on_change: Keyword.get(opts, :update_revision_on_change, false),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60),
      heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
      heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240),
      keep_latest_completed_computations: Keyword.get(opts, :keep_latest_completed_computations),
      keep_oldest_completed_computations: Keyword.get(opts, :keep_oldest_completed_computations)
    }
  end

  @doc """
  Creates a graph node that archives the execution when unblocked. Archived
  executions are excluded from `load/2` unless `include_archived: true` is passed.

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
  iex> execution = graph |> Journey.start()
  iex> execution.archived_at == nil
  true
  iex> execution = Journey.set(execution, :name, "Mario")
  iex> {:ok, _, _} = Journey.get(execution, :archive, wait: :any)
  iex> Journey.load(execution)
  nil
  iex> execution = Journey.load(execution, include_archived: true)
  iex> execution.archived_at == nil
  false
  ```

  ## Options

  - `:f_on_save` — see [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`).
    On success, `result` is `{:ok, archived_at}`.

  """
  def archive(name, gated_by, opts \\ [])
      when is_atom(name) do
    check_options(opts, @common_step_options)
    validate_retention_options(opts)

    %Graph.Step{
      name: name,
      type: :archive,
      gated_by: normalize_gated_by(gated_by),
      f_compute: &archive_graph/1,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60),
      heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
      heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240),
      keep_latest_completed_computations: Keyword.get(opts, :keep_latest_completed_computations),
      keep_oldest_completed_computations: Keyword.get(opts, :keep_oldest_completed_computations)
    }
  end

  defp archive_graph(e) do
    archived_at = Journey.Executions.archive_execution(e.execution_id)
    {:ok, archived_at}
  end

  @doc """
  Creates a history-tracking node that maintains a chronological log of changes to one or more nodes.

  `name` is an atom uniquely identifying this history node.

  `gated_by` defines which nodes to track. Accepts the same formats as `compute/4`:
    - A single-item list like `[:node_name]` to track one node
    - A list like `[:a, :b]` to track multiple nodes (all must be set)
    - Complex conditions using `unblocked_when/1` (e.g., `unblocked_when({:or, [{:a, &provided?/1}, {:b, &provided?/1}]})`)

  The historian will track changes to ALL nodes in the dependency tree and record only those that have changed since the last recording.

  ## Options
  - `:max_entries` (optional) - Maximum number of history entries to keep (FIFO).
    Defaults to 1000. Set to `nil` for unlimited history.
  - `:f_on_save` — see [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`).
    On success, `result` is `{:ok, history}`.

  ## History Format

  The history is returned as a list of entries in **newest-first order** (most recent changes at index 0).
  Each entry is a map containing:
  - `"value"` - The value of the changed node
  - `"node"` - The name of the node (as string)
  - `"timestamp"` - Unix timestamp when recorded
  - `"metadata"` - Metadata from the node (if any)
  - `"revision"` - Revision number of the node when recorded

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`historian()` doctest graph (tracks content changes)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:content),
  ...>         historian(:content_history, [:content])
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start()
  iex> execution = Journey.set(execution, :content, "First version")
  iex> {:ok, history1, history1_rev} = Journey.get(execution, :content_history, wait: :any)
  iex> length(history1)
  1
  iex> [%{"value" => "First version", "node" => "content", "timestamp" => _ts}] = history1
  iex> execution = Journey.set(execution, :content, "Second version")
  iex> {:ok, history2, _} = Journey.get(execution, :content_history, wait: {:newer_than, history1_rev})
  iex> length(history2)
  2
  iex> [%{"value" => "Second version", "node" => "content", "timestamp" => _ts}, _] = history2
  ```

  With custom max_entries limit:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "historian with max_entries",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:status),
  ...>         historian(:status_history, [:status], max_entries: 2)
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start()
  iex> execution = Journey.set(execution, :status, "pending")
  iex> {:ok, history, rev1} = Journey.get(execution, :status_history, wait: :any)
  iex> Enum.map(history, fn entry -> entry["value"] end)
  ["pending"]
  iex> execution = Journey.set(execution, :status, "active")
  iex> {:ok, history, rev2} = Journey.get(execution, :status_history, wait: {:newer_than, rev1})
  iex> Enum.map(history, fn entry -> entry["value"] end)
  ["active", "pending"]
  iex> execution = Journey.set(execution, :status, "completed")
  iex> {:ok, history, _rev} = Journey.get(execution, :status_history, wait: {:newer_than, rev2})
  iex> # Since status_history is limited to `max_entries: 2`, we'll only see the 2 latest values (newest first).
  iex> Enum.map(history, fn entry -> entry["value"] end)
  ["completed", "active"]

  ```

  With unlimited history:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "historian with unlimited history",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:audit_event),
  ...>         # Explicitly opt-in to unlimited history for audit trail
  ...>         historian(:audit_log, [:audit_event], max_entries: nil)
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start()
  iex> execution = Journey.set(execution, :audit_event, "login")
  iex> {:ok, history, _} = Journey.get(execution, :audit_log, wait: :any)
  iex> length(history)
  1
  iex> [%{"value" => "login", "node" => "audit_event", "timestamp" => _ts}] = history
  ```

  Tracking multiple nodes with `:or` condition:

  ```elixir
  iex> import Journey.Node
  iex> import Journey.Node.Conditions
  iex> import Journey.Node.UpstreamDependencies
  iex> graph = Journey.new_graph(
  ...>       "historian multi-node example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:email),
  ...>         input(:phone),
  ...>         # Track changes to either email or phone
  ...>         historian(
  ...>           :contact_history,
  ...>           unblocked_when({
  ...>             :or,
  ...>             [{:email, &provided?/1}, {:phone, &provided?/1}]
  ...>           })
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start()
  iex> execution = Journey.set(execution, :email, "user@example.com")
  iex> {:ok, history1, history1_rev} = Journey.get(execution, :contact_history, wait: :any)
  iex> length(history1)
  1
  iex> [%{"value" => "user@example.com", "node" => "email"}] = history1
  iex> execution = Journey.set(execution, :phone, "555-1234")
  iex> {:ok, history2, _} = Journey.get(execution, :contact_history, wait: {:newer_than, history1_rev})
  iex> length(history2)
  2
  iex> # Newest first: phone, then email
  iex> [%{"value" => "555-1234", "node" => "phone"}, %{"value" => "user@example.com", "node" => "email"}] = history2
  ```

  """
  def historian(name, gated_by, opts \\ []) when is_atom(name) do
    check_options(opts, @common_step_options ++ [:max_entries])
    validate_retention_options(opts)

    max_entries = Keyword.get(opts, :max_entries, 1000)

    %Graph.Step{
      name: name,
      type: :historian,
      gated_by: normalize_gated_by(gated_by),
      f_compute: fn inputs, value_nodes_map ->
        process_historian_update(inputs, value_nodes_map, name, max_entries)
      end,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60),
      heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
      heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240),
      keep_latest_completed_computations: Keyword.get(opts, :keep_latest_completed_computations),
      keep_oldest_completed_computations: Keyword.get(opts, :keep_oldest_completed_computations)
    }
  end

  defp process_historian_update(inputs, value_nodes_map, history_node_name, max_entries) do
    existing_history = Map.get(inputs, history_node_name, [])

    # Build a map of last recorded revisions for each node
    last_revisions =
      Enum.reduce(existing_history, %{}, fn entry, acc ->
        node_name = entry["node"]
        revision = entry["revision"]
        current_max = Map.get(acc, node_name, revision)
        Map.put(acc, node_name, max(current_max, revision))
      end)

    # Find all tracked nodes (exclude the historian node itself)
    tracked_nodes =
      value_nodes_map
      |> Map.keys()
      |> Enum.reject(fn node -> node == history_node_name end)

    # Create entries for nodes with new revisions
    new_entries =
      tracked_nodes
      |> Enum.filter(fn node ->
        current_revision = get_in(value_nodes_map, [node, :revision])
        last_revision = Map.get(last_revisions, to_string(node))

        # Include if node has value and (no previous revision or current is newer)
        Map.has_key?(inputs, node) and
          (is_nil(last_revision) or current_revision > last_revision)
      end)
      |> Enum.map(fn node ->
        %{
          "value" => Map.get(inputs, node),
          "node" => to_string(node),
          "timestamp" => System.system_time(:second),
          "metadata" => get_in(value_nodes_map, [node, :metadata]),
          "revision" => get_in(value_nodes_map, [node, :revision])
        }
      end)
      |> Enum.sort_by(
        fn entry ->
          {entry["revision"], entry["timestamp"], entry["node"]}
        end,
        :asc
      )

    # Prepend new entries (newest first)
    updated_history = new_entries ++ existing_history

    # Apply max_entries limit
    final_history =
      case max_entries do
        nil ->
          updated_history

        max when is_integer(max) and max > 0 ->
          Enum.take(updated_history, max)
      end

    {:ok, final_history}
  end

  @doc """
  Creates a graph node that declares its readiness at a specific time, once.

  Once this node is unblocked, it will be executed to set the time at which it will unblock its downstream dependencies.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`tick_once()` doctest graph (it reminds you to take a nap in a couple of seconds;)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         tick_once(
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
  ...>     |> Journey.start()
  ...>     |> Journey.set(:name, "Mario")
  iex> execution |> Journey.values() |> Map.get(:name)
  "Mario"
  iex> # This is only needed in a test, to simulate the background processing that happens in non-tests automatically.
  iex> background_sweeps_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)
  iex> {:ok, "It's time to take a nap, Mario!", _} = execution |> Journey.get(:nap_time, wait: :any)
  iex> Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_sweeps_task)

  ```

  ## Return Values
  The f_compute function must return `{:ok, value}` or `{:error, reason}`. Note that atoms
  in the returned `value` and `reason` will be converted to strings when persisted.

  Returning `{:ok, 0}` indicates that the schedule should not trigger downstream computations.
  This might be useful for conditional scheduling - when the function determines that
  scheduling is not needed based on the current state.

  ## Options

  - `:f_on_save` — see [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`).
    On success, `result` is `{:ok, schedule_time}` (the unix-epoch second
    returned by `f_compute`).

  ## Conditional Scheduling Example

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "conditional scheduling example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:user_name),
  ...>         input(:wants_reminder),
  ...>         tick_once(
  ...>           :schedule_reminder,
  ...>           [:user_name, :wants_reminder],
  ...>           fn %{wants_reminder: wants_reminder} ->
  ...>             if wants_reminder do
  ...>               # Schedule for 2 seconds from now
  ...>               {:ok, System.system_time(:second) + 2}
  ...>             else
  ...>               # Don't schedule - user opted out
  ...>               {:ok, 0}
  ...>             end
  ...>           end
  ...>         ),
  ...>         compute(
  ...>           :send_reminder,
  ...>           [:user_name, :schedule_reminder],
  ...>           fn %{user_name: name} ->
  ...>             {:ok, "Reminder for \#{name}"}
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start()
  iex> background_sweeps_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)
  iex> # User doesn't want reminders - schedule returns 0
  iex> execution = Journey.set(execution, :user_name, "Mario")
  iex> execution = Journey.set(execution, :wants_reminder, false)
  iex> {:ok, 0, _} = Journey.get(execution, :schedule_reminder, wait: :any)
  iex> # Reminder is never sent because schedule returned 0
  iex> :timer.sleep(3_000)
  iex> Journey.get(execution, :send_reminder)
  {:error, :not_set}
  iex> Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_sweeps_task)
  ```

  """
  def tick_once(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    check_options(opts, @common_step_options)
    validate_retention_options(opts)

    %Graph.Step{
      name: name,
      type: :tick_once,
      gated_by: normalize_gated_by(gated_by),
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60),
      heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
      heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240),
      keep_latest_completed_computations: Keyword.get(opts, :keep_latest_completed_computations),
      keep_oldest_completed_computations: Keyword.get(opts, :keep_oldest_completed_computations)
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
  ...>       "`tick_recurring()` doctest graph (it issues 'reminders' every few seconds)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         tick_recurring(
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
  ...>     |> Journey.start()
  ...>     |> Journey.set(:name, "Mario")
  iex> execution |> Journey.values() |> Map.get(:name)
  "Mario"
  iex> # This is only needed in a test, to simulate the background processing that happens in non-tests automatically.
  iex> background_sweeps_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)
  iex> # Wait for initial reminders
  iex> {:ok, count1, _} = Journey.get(execution, :send_a_reminder, wait: :any)
  iex> count1 >= 1
  true
  iex> # Wait for more reminders to verify recurring behavior
  iex> execution = Journey.load(execution)
  iex> {:ok, count2, _} = Journey.get(execution, :send_a_reminder, wait: :newer)
  iex> count2 > count1
  true
  iex> execution = Journey.load(execution)
  iex> {:ok, count3, _} = Journey.get(execution, :send_a_reminder, wait: :newer)
  iex> count3 > count2
  true
  iex> Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_sweeps_task)

  ```

  ## Return Values
  The f_compute function must return `{:ok, value}` or `{:error, reason}`. Note that atoms
  in the returned `value` and `reason` will be converted to strings when persisted.

  Returning `{:ok, 0}` indicates that the schedule should not trigger downstream computations.

  ## Options

  - `:f_on_save` — see [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`).
    On success, `result` is `{:ok, schedule_time}` (the unix-epoch second
    returned by `f_compute` for one scheduled fire).

  ## Computation Retention

  Recurring nodes accumulate computation records over time. Use `:keep_latest_completed_computations`
  to limit retention. See `compute/4` for details.

      tick_recurring(:schedule, deps, &next_time/1,
        keep_latest_completed_computations: 200
      )
  """

  def tick_recurring(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    check_options(opts, @common_step_options)
    validate_retention_options(opts)

    %Graph.Step{
      name: name,
      type: :tick_recurring,
      gated_by: normalize_gated_by(gated_by),
      f_compute: f_compute,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60),
      heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
      heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240),
      keep_latest_completed_computations: Keyword.get(opts, :keep_latest_completed_computations),
      keep_oldest_completed_computations: Keyword.get(opts, :keep_oldest_completed_computations)
    }
  end

  @doc """
  Creates a loop node — a self-iterating computation that threads state across iterations durably.

  `loop/4` is the durable analogue of Elixir's `Stream.unfold/2` and `Enum.reduce_while/3`:
  the step function `f_loop` is invoked repeatedly, threading state forward via
  `values_map.<name>`, until either the function signals completion or `:max_iterations` is reached.

  Each iteration is recorded as its own computation row, providing per-iteration durability:
  if a worker crashes mid-iteration, retries resume from the last successful iteration rather
  than restarting from iteration 1.

  `name` is an atom uniquely identifying the node in this graph.

  `gated_by` defines when the loop becomes eligible to start (same syntax as `compute/4`).

  `f_loop` is the step function. It can accept either one or two arguments:
   - **Arity 1**: `fn values_map -> ... end`
   - **Arity 2**: `fn values_map, value_nodes_map -> ... end`

  On each invocation:
   - `values_map` contains the current values of all upstream dependencies.
   - On the first iteration, the `<name>` key is absent from `values_map` — use bracket access
     (`values_map[:<name>]`) rather than dot access to read it safely.
   - On subsequent iterations, `values_map[:<name>]` reflects the value carried by the most recent
     `:cont_*` return.

  The function must return one of four tuples:

  | Return | Meaning |
  |---|---|
  | `{:ok, result}` | Iteration is complete; `result` becomes the loop's terminal value. Downstream nodes fire. |
  | `{:error, reason}` | Transient failure of this iteration. Engine retries per `:max_retries`. After exhaustion, the loop fails; the node's value remains unset and `f_on_save` fires with `{:error, reason}`. |
  | `{:cont_with_fallback, value}` | Continue iterating. If `:max_iterations` is reached before another resolution, `value` is promoted to the loop's terminal value. |
  | `{:cont_no_fallback, value}` | Continue iterating. If `:max_iterations` is reached, the loop fails with no usable result; the node's value remains unset and `f_on_save` fires with `{:error, "max_iterations_reached"}`. |

  ## Options

  - `:max_iterations` (required) — positive integer; the maximum number of iterations.
    `:infinity` is not allowed.
  - `:max_retries` (default: 3) — per-iteration retry budget for `{:error, _}` returns.
  - `:abandon_after_seconds` (default: 60) — per-iteration timeout.
  - `:f_on_save` — see [`f_on_save` callbacks](`Journey#module-f_on_save-callbacks`).
    Loop-specific result shapes:
    - `{:ok, value}` — terminal `:ok` from the step function, or
      cap-promoted `:cont_with_fallback` at `:max_iterations`.
    - `{:error, "max_iterations_reached"}` — cap-failed
      `:cont_no_fallback` at `:max_iterations`.

    For per-iteration progress visibility, use `Journey.Tools.introspect/1`.
  - Plus `:heartbeat_interval_seconds`, `:heartbeat_timeout_seconds`,
    `:keep_latest_completed_computations`, `:keep_oldest_completed_computations` (same semantics
    as `compute/4`).

  ## Visibility

  The loop node's value (visible via `Journey.values/1`, `unblocked_when` predicates,
  `f_on_save` callbacks) is set in exactly two cases:
   1. The step function returns `{:ok, value}`.
   2. `:max_iterations` is reached and the most recent return was `{:cont_with_fallback, value}`.

  A loop node's value reflects the **last successful write**, not the outcome of the
  current run — even across re-runs triggered by upstream changes.

  During iteration, the loop's accumulating state is visible only to the step function itself
  (via `values_map[:<name>]`); it is not externally visible until terminal resolution.

  ## Carried values go through JSONB

  The `value` in `{:cont_with_fallback, value}` / `{:cont_no_fallback, value}` is persisted as
  JSON between iterations, following the same rules as `Journey.set/3`: atoms become strings,
  and map keys must be strings. Use strings (not atoms) for any value you intend to
  pattern-match on in a later iteration — e.g. prefer `{:cont_with_fallback, "keep_going"}`
  over `{:cont_with_fallback, :keep_going}`, since the latter will be read back as
  `"keep_going"` on the next iteration.

  ## At-least-once semantics

  Step functions are invoked at-least-once, identical to `compute/4`. Side effects with
  irreversible external impact (sending email, charging a card) should be performed by nodes
  downstream of the loop's terminal value, not inside the step function itself.

  ## Example: iteratively shrink a string until it fits

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "`loop()` doctest graph (shrink a string)",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:document),
  ...>         input(:target_length),
  ...>         loop(
  ...>           :summary,
  ...>           [:document, :target_length],
  ...>           fn values ->
  ...>             current = values[:summary] || values.document
  ...>
  ...>             cond do
  ...>               String.length(current) <= values.target_length ->
  ...>                 {:ok, current}
  ...>
  ...>               String.length(current) <= 1 ->
  ...>                 {:ok, current}
  ...>
  ...>               true ->
  ...>                 shorter = String.slice(current, 0, div(String.length(current), 2))
  ...>                 {:cont_with_fallback, shorter}
  ...>             end
  ...>           end,
  ...>           max_iterations: 10
  ...>         )
  ...>       ]
  ...>     )
  iex> execution =
  ...>   graph
  ...>   |> Journey.start()
  ...>   |> Journey.set(:document, "abcdefghijklmnop")
  ...>   |> Journey.set(:target_length, 4)
  iex> {:ok, "abcd", _} = Journey.get(execution, :summary, wait: :any)
  ```
  """
  def loop(name, gated_by, f_loop, opts \\ [])
      when is_atom(name) and is_function(f_loop) do
    check_options(opts, @loop_options)
    validate_retention_options(opts)

    max_iterations =
      case Keyword.fetch(opts, :max_iterations) do
        {:ok, n} when is_integer(n) and n > 0 ->
          n

        {:ok, other} ->
          raise ArgumentError,
                "max_iterations must be a positive integer, got: #{inspect(other)}"

        :error ->
          raise ArgumentError, "loop/4 requires the :max_iterations option (a positive integer)"
      end

    %Graph.Step{
      name: name,
      type: :loop,
      gated_by: normalize_gated_by(gated_by),
      f_compute: f_loop,
      f_on_save: Keyword.get(opts, :f_on_save, nil),
      max_iterations: max_iterations,
      max_retries: Keyword.get(opts, :max_retries, 3),
      abandon_after_seconds: Keyword.get(opts, :abandon_after_seconds, 60),
      heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
      heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240),
      keep_latest_completed_computations: Keyword.get(opts, :keep_latest_completed_computations),
      keep_oldest_completed_computations: Keyword.get(opts, :keep_oldest_completed_computations)
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

  def redact(map, keys) when is_map(map) and is_list(keys) do
    keys
    |> Enum.reduce(
      map,
      fn key, acc when is_atom(key) and is_map(acc) ->
        redact(acc, key)
      end
    )
  end

  defp normalize_gated_by(gated_by) when is_list(gated_by) do
    import Journey.Node.Conditions, only: [provided?: 1]

    conditions =
      Enum.map(gated_by, fn
        atom when is_atom(atom) ->
          {atom, &provided?/1}

        {node_name, condition_fn} when is_atom(node_name) and is_function(condition_fn, 1) ->
          {node_name, fn node -> node.set_time != nil and condition_fn.(node) end}
      end)

    {:and, conditions}
  end

  defp normalize_gated_by(gated_by) do
    # Not a list - pass through unchanged (handles tuples like unblocked_when/2 results)
    gated_by
  end

  # Deprecated aliases for backward compatibility
  @doc group: "Deprecated"
  @deprecated "Use tick_once/4 instead"
  def schedule_once(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    tick_once(name, gated_by, f_compute, opts)
  end

  @doc group: "Deprecated"
  @deprecated "Use tick_recurring/4 instead"
  def schedule_recurring(name, gated_by, f_compute, opts \\ [])
      when is_atom(name) and is_function(f_compute) do
    tick_recurring(name, gated_by, f_compute, opts)
  end

  # Validates that only known option keys are provided
  defp check_options(supplied_opts, known_opts) do
    supplied = MapSet.new(Keyword.keys(supplied_opts))
    known = MapSet.new(known_opts)
    unexpected = MapSet.difference(supplied, known)

    if unexpected != MapSet.new([]) do
      raise ArgumentError,
            "Unknown options: #{inspect(Enum.sort(MapSet.to_list(unexpected)))}. Known options: #{inspect(Enum.sort(known_opts))}."
    end
  end

  defp validate_retention_options(opts) do
    validate_keep_latest!(Keyword.get(opts, :keep_latest_completed_computations))
    validate_keep_oldest!(Keyword.get(opts, :keep_oldest_completed_computations))
  end

  defp validate_keep_latest!(nil), do: :ok
  defp validate_keep_latest!(:all), do: :ok
  defp validate_keep_latest!(n) when is_integer(n) and n > 0, do: :ok

  defp validate_keep_latest!(other) do
    raise ArgumentError,
          "keep_latest_completed_computations must be :all or a positive integer, got: #{inspect(other)}"
  end

  defp validate_keep_oldest!(nil), do: :ok
  defp validate_keep_oldest!(n) when is_integer(n) and n > 0, do: :ok

  defp validate_keep_oldest!(other) do
    raise ArgumentError,
          "keep_oldest_completed_computations must be a positive integer, got: #{inspect(other)}"
  end
end
