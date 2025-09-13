defmodule Journey do
  @moduledoc """

  This module is the entry point for the Journey library. It provides functions for creating and managing computation graphs, starting and managing executions, and retrieving values from executions.

  Here is a quick example of how to use the library, illustrating the basic concepts of defining a graph, starting an execution of the graph, and setting input values and getting computed values.

  ```elixir
  iex> # 1. Define a graph capturing the data and the logic of the application -
  iex> #    the nodes, their dependencies, and their computations:
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "horoscope workflow - module doctest",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:first_name),
  ...>         input(:birth_day),
  ...>         input(:birth_month),
  ...>         compute(
  ...>           :zodiac_sign,
  ...>           # Depends on user-supplied data:
  ...>           [:birth_month, :birth_day],
  ...>           # Computes itself, once the dependencies are satisfied:
  ...>           fn %{birth_month: _birth_month, birth_day: _birth_day} ->
  ...>             {:ok, "Taurus"}
  ...>           end
  ...>         ),
  ...>         compute(
  ...>           :horoscope,
  ...>           # Computes itself once :first_name and :zodiac_sign are in place:
  ...>           [:first_name, :zodiac_sign],
  ...>           fn %{first_name: name, zodiac_sign: zodiac_sign} ->
  ...>             {:ok, "🍪s await, \#{zodiac_sign} \#{name}!"}
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex>
  iex> # 2. For every customer visiting your website, start a new execution of the graph:
  iex> e = Journey.start_execution(graph)
  iex>
  iex> # 3. Populate the execution's nodes with the data as provided by the visitor:
  iex> e = Journey.set_value(e, :birth_day, 26)
  iex>
  iex> # As a side note: if the user leaves and comes back later or if everything crashes,
  iex> # you can always reload the execution using its id:
  iex> e = Journey.load(e.id)
  iex>
  iex> # Continuing, as if nothing happened:
  iex> e = Journey.set_value(e, :birth_month, "April")
  iex>
  iex> # 4. Now that we have :birth_month and :birth_day, :zodiac_sign will compute itself:
  iex> Journey.get_value(e, :zodiac_sign, wait_any: true)
  {:ok, "Taurus"}
  iex> Journey.values(e) |> redact([:execution_id, :last_updated_at])
  %{birth_day: 26, birth_month: "April", zodiac_sign: "Taurus", execution_id: "...", last_updated_at: 1234567890}
  iex>
  iex> # 5. Once we get :first_name, the :horoscope node will compute itself:
  iex> e = Journey.set_value(e, :first_name, "Mario")
  iex> Journey.get_value(e, :horoscope, wait_any: true)
  {:ok, "🍪s await, Taurus Mario!"}
  iex>
  iex> Journey.values(e) |> redact([:execution_id, :last_updated_at])
  %{birth_day: 26, birth_month: "April", first_name: "Mario", horoscope: "🍪s await, Taurus Mario!", zodiac_sign: "Taurus", execution_id: "...", last_updated_at: 1234567890}
  iex>
  iex> # 6. and we can always list executions.
  iex> this_execution = Journey.list_executions(graph_name: "horoscope workflow - module doctest", order_by_execution_fields: [:inserted_at]) |> Enum.reverse() |> hd
  iex> e.id == this_execution.id
  true
  ```

  """

  alias Journey.Executions
  alias Journey.Graph
  alias Journey.Persistence.Schema.Execution

  @doc """
  Creates a new computation graph with the given name, version, and node definitions.

  The foundational function for defining Journey graphs. Creates a validated graph structure
  for starting executions. Defines data flow, dependencies, and computations for workflows.

  ## Quick Example

  ```elixir
  import Journey.Node
  graph = Journey.new_graph(
    "user onboarding",
    "v1.0.0",
    [
      input(:email),
      compute(:welcome_message, [:email], fn %{email: email} ->
        {:ok, "Welcome \#{email}!"}
      end)
    ]
  )
  execution = Journey.start_execution(graph)
  ```

  ## Parameters
  * `name` (string) - Graph identifier (e.g., "user registration workflow")
  * `version` (string) - Semantic version (e.g., "v1.0.0")
  * `nodes` (list) - Node definitions from `Journey.Node` functions (`input/1`, `compute/4`, etc.)
  * `opts` (keyword) - Options:
    * `:f_on_save` - Graph callback: `(execution_id, node_name, result) → :ok`
      Called after node-specific callbacks. Result: `{:ok, value}` | `{:error, reason}`

  ## Returns
  * `%Journey.Graph{}` - Validated and registered computation graph

  ## Behavior & Errors
  * Validates structure (cycles, dependencies) - raises `RuntimeError` on failure
  * Registers in catalog for tracking and reloading
  * Immutable once created - create new versions for changes
  * Supports all node types: input, compute, mutate, schedule_once, schedule_recurring
  * Graph-wide `f_on_save` called after node-specific callbacks
  * Raises `ArgumentError` for invalid parameters; `KeywordValidator.Error` for invalid options

  ## Examples

  Basic workflow with input and computation:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "greeting workflow",
  ...>   "v1.0.0",
  ...>   [
  ...>     input(:name),
  ...>     compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello, \#{name}!"} end)
  ...>   ]
  ...> )
  iex> graph.name
  "greeting workflow"
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set_value(execution, :name, "Alice")
  iex> Journey.get_value(execution, :greeting, wait_any: true)
  {:ok, "Hello, Alice!"}
  ```

  Graph with a graph-wide `f_on_save` callback:

  ```elixir
  iex> import Journey.Node
  iex> _graph = Journey.new_graph(
  ...>   "notification workflow",
  ...>   "v1.0.0",
  ...>   [
  ...>     input(:user_id),
  ...>     compute(:fetch_user, [:user_id], fn %{user_id: id} ->
  ...>       {:ok, %{id: id, name: "User \#{id}"}}
  ...>     end),
  ...>     compute(:send_email, [:fetch_user], fn %{fetch_user: user} ->
  ...>       {:ok, "Email sent to \#{user.name}"}
  ...>     end)
  ...>   ],
  ...>   f_on_save: fn _execution_id, node_name, result ->
  ...>     # This will be called for both :fetch_user and :send_email computations
  ...>     IO.puts("Node \#{node_name} completed with result: \#{inspect(result)}")
  ...>     :ok
  ...>   end
  ...> )
  ```

  Complex workflow with conditional dependencies:

  ```elixir
  iex> import Journey.Node
  iex> import Journey.Node.Conditions
  iex> import Journey.Node.UpstreamDependencies
  iex> graph = Journey.new_graph(
  ...>       "horoscope workflow",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:first_name),
  ...>         input(:birth_day),
  ...>         input(:birth_month),
  ...>         compute(
  ...>           :zodiac_sign,
  ...>           [:birth_month, :birth_day],
  ...>           fn %{birth_month: _birth_month, birth_day: _birth_day} ->
  ...>             {:ok, "Taurus"}
  ...>           end
  ...>         ),
  ...>         compute(
  ...>           :horoscope,
  ...>           unblocked_when({
  ...>             :and,
  ...>             [
  ...>               {:first_name, &provided?/1},
  ...>               {:zodiac_sign, &provided?/1}
  ...>             ]
  ...>           }),
  ...>           fn %{first_name: name, zodiac_sign: zodiac_sign} ->
  ...>             {:ok, "🍪s await, \#{zodiac_sign} \#{name}!"}
  ...>           end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set_value(execution, :birth_day, 15)
  iex> execution = Journey.set_value(execution, :birth_month, "May")
  iex> Journey.get_value(execution, :zodiac_sign, wait_any: true)
  {:ok, "Taurus"}
  iex> execution = Journey.set_value(execution, :first_name, "Bob")
  iex> Journey.get_value(execution, :horoscope, wait_any: true)
  {:ok, "🍪s await, Taurus Bob!"}
  ```

  Multiple node types in a workflow:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "data processing workflow",
  ...>   "v2.1.0",
  ...>   [
  ...>     input(:raw_data),
  ...>     compute(:upper_case, [:raw_data], fn %{raw_data: data} ->
  ...>       {:ok, String.upcase(data)}
  ...>     end),
  ...>     compute(:suffix, [:upper_case], fn %{upper_case: data} ->
  ...>       {:ok, "\#{data} omg yay"}
  ...>     end)
  ...>   ]
  ...> )
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set_value(execution, :raw_data, "hello world")
  iex> Journey.get_value(execution, :upper_case, wait_any: true)
  {:ok, "HELLO WORLD"}
  iex> Journey.get_value(execution, :suffix, wait_any: true)
  {:ok, "HELLO WORLD omg yay"}
  ```

  """
  def new_graph(name, version, nodes, opts \\ [])
      when is_binary(name) and is_binary(version) and is_list(nodes) do
    Graph.new(name, version, nodes, opts)
    |> Journey.Graph.Validations.validate()
    |> Graph.Catalog.register()
  end

  @doc """
  Reloads the current state of an execution from the database to get the latest changes.

  Fetches latest execution state including changes from background computations, scheduled events,
  or concurrent processes. Essential for resuming sessions after restarts or when users return.

  ## Quick Example

  ```elixir
  execution = Journey.set_value(execution, :name, "Mario")
  execution = Journey.load(execution)  # Get updated state with new revision
  {:ok, greeting} = Journey.get_value(execution, :greeting, wait_any: true)
  ```

  ## Parameters
  * `execution` - `%Execution{}` struct or execution ID (string)
  * `opts` (keyword) - Options:
    * `:preload` (boolean) - Preload nodes/values (default: true). Use false for metadata-only performance.
    * `:include_archived` (boolean) - Include archived executions (default: false)

  ## Returns
  * `%Execution{}` with current database state, or `nil` if not found

  ## Behavior
  * Always returns fresh database state (not cached)
  * Includes latest revision number
  * Archived executions return `nil` unless explicitly included

  **See also:** `set_value/3`, `get_value/3`

  ## Examples

  Basic reloading after value changes:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "load example - basic",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello, \#{name}!"} end)
  ...>       ]
  ...>     )
  iex> execution = Journey.start_execution(graph)
  iex> execution.revision
  0
  iex> execution = Journey.set_value(execution, :name, "Alice")
  iex> execution.revision > 0
  true
  iex> {:ok, "Hello, Alice!"} = Journey.get_value(execution, :greeting, wait_any: true)
  iex> reloaded = Journey.load(execution)
  iex> reloaded.revision >= execution.revision
  true
  ```

  Loading by execution ID:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "load example - by id",
  ...>       "v1.0.0",
  ...>       [input(:data)]
  ...>     )
  iex> execution = Journey.start_execution(graph)
  iex> execution_id = execution.id
  iex> reloaded = Journey.load(execution_id)
  iex> reloaded.id == execution_id
  true
  ```

  Performance optimization with preload option:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "load example - no preload",
  ...>       "v1.0.0",
  ...>       [input(:data)]
  ...>     )
  iex> execution = Journey.start_execution(graph)
  iex> fast_load = Journey.load(execution, preload: false)
  iex> fast_load.id == execution.id
  true
  ```

  Handling archived executions:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "load example - archived",
  ...>       "v1.0.0",
  ...>       [input(:data)]
  ...>     )
  iex> execution = Journey.start_execution(graph)
  iex> Journey.archive(execution)
  iex> Journey.load(execution)
  nil
  iex> Journey.load(execution, include_archived: true) != nil
  true
  ```

  """
  def load(_, _opts \\ [])

  def load(nil, _), do: nil

  def load(execution_id, opts) when is_binary(execution_id) do
    opts_schema = [
      preload: [is: :boolean],
      include_archived: [is: :boolean]
    ]

    KeywordValidator.validate!(opts, opts_schema)

    Journey.Executions.load(
      execution_id,
      Keyword.get(opts, :preload, true),
      Keyword.get(opts, :include_archived, false)
    )
  end

  def load(execution, opts) when is_struct(execution, Execution) do
    load(execution.id, opts)
  end

  @doc """
  Queries and retrieves multiple executions from the database with flexible filtering, sorting, and pagination.

  Enables searching across all executions with powerful filtering on graph names, node values,
  and execution metadata. Essential for monitoring workflows, dashboards, and analytics.

  ## Quick Example

  ```elixir
  # List all executions for a specific graph
  executions = Journey.list_executions(graph_name: "user_onboarding")

  # List executions for a specific graph version
  v1_executions = Journey.list_executions(
    graph_name: "user_onboarding",
    graph_version: "v1.0.0"
  )

  # Find executions where age > 18
  adults = Journey.list_executions(
    graph_name: "user_registration",
    filter_by: [{:age, :gt, 18}]
  )
  ```

  ## Parameters
  * `options` (keyword) - All optional:
    * `:graph_name` (string) - Filter by graph
    * `:graph_version` (string) - Filter by version (requires :graph_name)
    * `:sort_by` (list) - Sort fields: execution fields or node values [→ Sorting]
    * `:filter_by` (list) - Node filters: `{node, op, value}` where op ∈ {:eq, :neq, :lt, :lte, :gt, :gte, :in, :not_in, :is_nil, :is_not_nil}. Primitives only. [→ Filtering]
    * `:limit` (integer) - Max results (default: 10,000)
    * `:offset` (integer) - Skip for pagination (default: 0)
    * `:include_archived` (boolean) - Include archived (default: false)

  ## Returns
  * List of `%Execution{}` structs with preloaded values/computations, or `[]` if none match

  ### Sorting
  Sort by execution fields or node values: `[:updated_at]`, `[updated_at: :desc]`, mixed formats.
  * Execution fields: `:inserted_at`, `:updated_at`, `:revision`, `:graph_name`, `:graph_version`
  * Node values: Any graph node (e.g., `:age`, `:score`) - JSONB ordering
  * Direction: `:asc` (default) | `:desc`

  ### Filtering
  Database-level filtering for performance. Primitive values only (complex types raise errors).
  Archived executions excluded by default.

  **See also:** `start_execution/1`, `load/2`

  ## Examples

  Basic listing by graph name:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "list example basic - \#{Journey.Helpers.Random.random_string()}",
  ...>   "v1.0.0",
  ...>   [input(:status)]
  ...> )
  iex> Journey.start_execution(graph) |> Journey.set_value(:status, "active")
  iex> Journey.start_execution(graph) |> Journey.set_value(:status, "pending")
  iex> executions = Journey.list_executions(graph_name: graph.name)
  iex> length(executions)
  2
  ```

  Filtering by graph version:

  ```elixir
  iex> import Journey.Node
  iex> graph_name = "version example #{Journey.Helpers.Random.random_string()}"
  iex> graph_v1 = Journey.new_graph(
  ...>   graph_name,
  ...>   "v1.0.0",
  ...>   [input(:data)]
  ...> )
  iex> graph_v2 = Journey.new_graph(
  ...>   graph_name,
  ...>   "v2.0.0",
  ...>   [input(:data), input(:new_field)]
  ...> )
  iex> Journey.start_execution(graph_v1) |> Journey.set_value(:data, "v1 data")
  iex> Journey.start_execution(graph_v2) |> Journey.set_value(:data, "v2 data")
  iex> Journey.list_executions(graph_name: graph_v1.name, graph_version: "v1.0.0") |> length()
  1
  iex> Journey.list_executions(graph_name: graph_v1.name, graph_version: "v2.0.0") |> length()
  1
  iex> Journey.list_executions(graph_name: graph_v1.name) |> length()
  2
  ```


  Sorting:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "sort example - \#{Journey.Helpers.Random.random_string()}",
  ...>   "v1.0.0",
  ...>   [input(:priority)]
  ...> )
  iex> Journey.start_execution(graph) |> Journey.set_value(:priority, "high")
  iex> Journey.start_execution(graph) |> Journey.set_value(:priority, "low")
  iex> Journey.start_execution(graph) |> Journey.set_value(:priority, "medium")
  iex> # Sort by priority descending - shows the actual sorted values
  iex> Journey.list_executions(graph_name: graph.name, sort_by: [priority: :desc]) |> Enum.map(fn e -> Journey.values(e) |> Map.get(:priority) end)
  ["medium", "low", "high"]
  ```

  Filtering:

  ```elixir
  iex> graph = Journey.Examples.Horoscope.graph()
  iex> for day <- 1..20, do: Journey.start_execution(graph) |> Journey.set_value(:birth_day, day) |> Journey.set_value(:birth_month, 4) |> Journey.set_value(:first_name, "Mario")
  iex> # Various filtering examples
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:birth_day, :eq, 10}]) |> Enum.count()
  1
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:birth_day, :neq, 10}]) |> Enum.count()
  19
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:birth_day, :lte, 5}]) |> Enum.count()
  5
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:birth_day, :in, [5, 10, 15]}]) |> Enum.count()
  3
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :is_not_nil}]) |> Enum.count()
  20
  ```

  Multiple filters, sorting, and pagination:

  ```elixir
  iex> graph = Journey.Examples.Horoscope.graph()
  iex> for day <- 1..20, do: Journey.start_execution(graph) |> Journey.set_value(:birth_day, day) |> Journey.set_value(:birth_month, 4) |> Journey.set_value(:first_name, "Mario")
  iex> # Multiple filters combined
  iex> Journey.list_executions(
  ...>   graph_name: graph.name,
  ...>   filter_by: [{:birth_day, :gt, 10}, {:first_name, :is_not_nil}],
  ...>   sort_by: [birth_day: :desc],
  ...>   limit: 5
  ...> ) |> Enum.count()
  5
  iex> # Pagination
  iex> Journey.list_executions(graph_name: graph.name, limit: 3) |> Enum.count()
  3
  iex> Journey.list_executions(graph_name: graph.name, limit: 5, offset: 10) |> Enum.count()
  5
  ```

  Including archived executions:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "list example - archived - \#{Journey.Helpers.Random.random_string()}",
  ...>   "v1.0.0",
  ...>   [input(:status)]
  ...> )
  iex> e1 = Journey.start_execution(graph)
  iex> _e2 = Journey.start_execution(graph)
  iex> Journey.archive(e1)
  iex> Journey.list_executions(graph_name: graph.name) |> length()
  1
  iex> Journey.list_executions(graph_name: graph.name, include_archived: true) |> length()
  2
  ```

  """

  def list_executions(options \\ []) do
    check_options(options, [
      :graph_name,
      :graph_version,
      :sort_by,
      # Undocumented alias for backwards compatibility
      :order_by_execution_fields,
      :filter_by,
      # Deprecated alias for backwards compatibility
      :value_filters,
      :limit,
      :offset,
      :include_archived
    ])

    # Handle filter_by taking precedence over value_filters (deprecated)
    filter_by = options[:filter_by] || options[:value_filters] || []
    limit = Keyword.get(options, :limit, 10_000)
    offset = Keyword.get(options, :offset, 0)

    graph_name = Keyword.get(options, :graph_name, nil)
    graph_version = Keyword.get(options, :graph_version, nil)

    # Handle sort_by taking precedence over order_by_execution_fields
    sort_by = options[:sort_by] || options[:order_by_execution_fields] || [:updated_at]

    include_archived = Keyword.get(options, :include_archived, false)

    # Validate that graph_version requires graph_name
    if graph_version != nil and graph_name == nil do
      raise ArgumentError, "Option :graph_version requires :graph_name to be specified"
    end

    Journey.Executions.list(graph_name, graph_version, sort_by, filter_by, limit, offset, include_archived)
  end

  @doc """
  Starts a new execution instance of a computation graph, initializing it to accept input values and perform computations.

  Creates persistent execution in database with unique ID. Begins background processing for
  schedulable nodes. Starts with revision 0 and no values set.

  ## Quick Example

  ```elixir
  execution = Journey.start_execution(graph)
  execution = Journey.set_value(execution, :name, "Mario")
  {:ok, greeting} = Journey.get_value(execution, :greeting, wait_any: true)
  ```

  ## Parameters
  * `graph` - Validated `%Journey.Graph{}` from `new_graph/3` (must be registered)

  ## Returns
  * New `%Execution{}` with:
    * `:id` - Unique identifier (UUID)
    * `:graph_name`, `:graph_version` - From source graph
    * `:revision` - Starts at 0, increments with changes
    * `:archived_at` - Initially nil

  ## Behavior
  * Immediately persisted to PostgreSQL
  * Each call creates independent execution
  * Scheduler monitors schedulable nodes
  * Ready for input values

  **See also:** `set_value/3`, `get_value/3`

  ## Examples

  Basic execution creation:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "greeting workflow",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         compute(
  ...>           :greeting,
  ...>           [:name],
  ...>           fn %{name: name} -> {:ok, "Hello, \#{name}!"} end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = Journey.start_execution(graph)
  iex> execution.graph_name
  "greeting workflow"
  iex> execution.graph_version
  "v1.0.0"
  iex> execution.revision
  0
  ```

  Execution properties and immediate workflow:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "calculation workflow",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:x),
  ...>         input(:y),
  ...>         compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end)
  ...>       ]
  ...>     )
  iex> execution = Journey.start_execution(graph)
  iex> is_binary(execution.id)
  true
  iex> execution.archived_at
  nil
  iex> user_values = Journey.values(execution, reload: false) |> Map.drop([:execution_id, :last_updated_at])
  iex> user_values
  %{}
  iex> execution = Journey.set_value(execution, :x, 10)
  iex> execution = Journey.set_value(execution, :y, 20)
  iex> Journey.get_value(execution, :sum, wait_any: true)
  {:ok, 30}
  ```

  Multiple independent executions:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "counter workflow",
  ...>       "v1.0.0",
  ...>       [input(:count)]
  ...>     )
  iex> execution1 = Journey.start_execution(graph)
  iex> execution2 = Journey.start_execution(graph)
  iex> execution1.id != execution2.id
  true
  iex> execution1 = Journey.set_value(execution1, :count, 1)
  iex> execution2 = Journey.set_value(execution2, :count, 2)
  iex> Journey.get_value(execution1, :count)
  {:ok, 1}
  iex> Journey.get_value(execution2, :count)
  {:ok, 2}
  ```

  """
  def start_execution(graph) when is_struct(graph, Graph) do
    Journey.Executions.create_new(
      graph.name,
      graph.version,
      graph.nodes,
      graph.hash
    )
    |> Journey.Scheduler.advance()
  end

  @doc """
  Returns a map of all nodes in an execution with their current status, including unset nodes.

  Unlike `values/2` which only returns set nodes, this shows all nodes. Unset nodes: `:not_set`,
  set nodes: `{:set, value}`. Useful for debugging and introspection.

  ## Quick Example

  ```elixir
  all_status = Journey.values_all(execution)
  # %{name: {:set, "Alice"}, age: :not_set, execution_id: {:set, "EXEC..."}, ...}
  ```

  ## Parameters
  * `execution` - `%Execution{}` struct
  * `opts` (keyword) - `:reload` (boolean, default: true) for fresh database state

  ## Returns
  * Map with all nodes: `:not_set` | `{:set, value}` (includes all graph nodes)

  **See also:** `values/2`, `get_value/3`

  ## Examples

  Basic usage showing status progression:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph("example", "v1.0.0", [input(:name), input(:age)])
  iex> execution = Journey.start_execution(graph)
  iex> Journey.values_all(execution) |> redact([:execution_id, :last_updated_at])
  %{name: :not_set, age: :not_set, execution_id: {:set, "..."}, last_updated_at: {:set, 1234567890}}
  iex> execution = Journey.set_value(execution, :name, "Alice")
  iex> Journey.values_all(execution) |> redact([:execution_id, :last_updated_at])
  %{name: {:set, "Alice"}, age: :not_set, execution_id: {:set, "..."}, last_updated_at: {:set, 1234567890}}
  ```

  """
  def values_all(execution, opts \\ []) when is_struct(execution, Execution) do
    opts_schema = [
      reload: [is: :boolean]
    ]

    KeywordValidator.validate!(opts, opts_schema)

    reload? = Keyword.get(opts, :reload, true)

    execution =
      if reload? do
        Journey.load(execution)
      else
        execution
      end

    Executions.values(execution)
  end

  @doc """
  Returns a map of all set node values in an execution, excluding unset nodes.

  Filters execution to only populated nodes. Always includes `:execution_id` and
  `:last_updated_at` metadata.

  ## Quick Example

  ```elixir
  execution = Journey.set_value(execution, :name, "Alice")
  values = Journey.values(execution)
  # %{name: "Alice", execution_id: "EXEC...", last_updated_at: 1234567890}
  ```

  ## Parameters
  * `execution` - `%Execution{}` struct
  * `opts` (keyword) - `:reload` (boolean) - see `values_all/1`

  ## Returns
  * Map with set nodes only (excludes `:not_set` nodes)

  **See also:** `values_all/1`, `get_value/3`

  ## Examples

  Basic usage:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph("example", "v1.0.0", [input(:name), input(:age)])
  iex> execution = Journey.start_execution(graph)
  iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
  %{execution_id: "...", last_updated_at: 1234567890}
  iex> execution = Journey.set_value(execution, :name, "Alice")
  iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
  %{name: "Alice", execution_id: "...", last_updated_at: 1234567890}
  ```

  """
  def values(execution, opts \\ []) when is_struct(execution, Execution) and is_list(opts) do
    opts_schema = [
      reload: [is: :boolean]
    ]

    KeywordValidator.validate!(opts, opts_schema)

    reload? = Keyword.get(opts, :reload, true)

    execution =
      if reload? do
        Journey.load(execution)
      else
        execution
      end

    execution
    |> values_all()
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

  @doc """
  Returns the chronological history of all successful computations and set values for an execution.

  Provides visibility into operation order during execution. Shows value sets and successful
  computations chronologically. Failed computations filtered out. At same revision:
  computations before values.

  ## Quick Example

  ```elixir
  history = Journey.history(execution)
  # [%{node_name: :x, computation_or_value: :value, revision: 1},
  #  %{node_name: :sum, computation_or_value: :computation, revision: 2}, ...]
  ```

  ## Parameters
  * `execution` - `%Execution{}` struct or execution ID (string)

  ## Returns
  * List of maps sorted by revision:
    * `:computation_or_value` - `:computation` | `:value`
    * `:node_name` - Node name
    * `:node_type` - `:input` | `:compute` | `:mutate` | etc.
    * `:revision` - Execution revision when completed
    * `:value` - Actual value (`:value` entries only)

  **See also:** `values/2`, `set_value/3`, `get_value/3`

  ## Examples

  Basic usage showing value sets and computation:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph("history example", "v1.0.0", [
  ...>   input(:x),
  ...>   input(:y),
  ...>   compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end)
  ...> ])
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set_value(execution, :x, 10)
  iex> execution = Journey.set_value(execution, :y, 20)
  iex> Journey.get_value(execution, :sum, wait_any: true)
  {:ok, 30}
  iex> Journey.history(execution) |> Enum.map(fn entry ->
  ...>   case entry.node_name do
  ...>     :execution_id -> %{entry | value: "..."}
  ...>     :last_updated_at -> %{entry | value: 1234567890}
  ...>     _ -> entry
  ...>   end
  ...> end)
  [
    %{node_name: :execution_id, node_type: :input, computation_or_value: :value, value: "...", revision: 0},
    %{node_name: :x, node_type: :input, computation_or_value: :value, value: 10, revision: 1},
    %{node_name: :y, node_type: :input, computation_or_value: :value, value: 20, revision: 2},
    %{node_name: :sum, node_type: :compute, computation_or_value: :computation, revision: 4},
    %{node_name: :last_updated_at, node_type: :input, computation_or_value: :value, value: 1234567890, revision: 4},
    %{node_name: :sum, node_type: :compute, computation_or_value: :value, value: 30, revision: 4}
  ]
  ```

  """
  def history(execution_id) when is_binary(execution_id) do
    Journey.Executions.history(execution_id)
  end

  def history(execution) when is_struct(execution, Execution) do
    Journey.Executions.history(execution.id)
  end

  @doc """
  Sets the value for an input node in an execution and triggers recomputation of dependent nodes.

  Automatically recomputes dependent nodes for consistency. Idempotent - setting the same value
  twice has no effect.

  ## Parameters
  * `execution` - `%Execution{}` struct or execution ID (string)
  * `node_name` (atom) - Input node name (must exist in graph)
  * `value` - Value to set: nil | string | number | map | list | boolean
    Note: atoms in maps/lists converted to strings

  ## Returns
  * Updated `%Execution{}` with incremented revision (if changed)

  ## Behavior & Errors
  * Triggers recomputation of dependent nodes (automatic cascade)
  * Idempotent - same value → no revision change
  * Input nodes only - raises `RuntimeError` for compute nodes or unknown nodes

  ## Quick Example

  ```elixir
  execution = Journey.set_value(execution, :name, "Mario")
  {:ok, greeting} = Journey.get_value(execution, :greeting, wait_any: true)
  ```

  **See also:** `get_value/3`, `unset_value/2`

  ## Examples

  Basic setting with cascading recomputation:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "set workflow - cascading example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello, \#{name}!"} end)
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set_value(execution, :name, "Mario")
  iex> Journey.get_value(execution, :greeting, wait_any: true)
  {:ok, "Hello, Mario!"}
  iex> execution = Journey.set_value(execution, :name, "Luigi")
  iex> Journey.get_value(execution, :greeting, wait_new: true)
  {:ok, "Hello, Luigi!"}
  ```

  Idempotent behavior - same value doesn't change revision:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "set workflow - idempotent example",
  ...>       "v1.0.0",
  ...>       [input(:name)]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set_value(execution, :name, "Mario")
  iex> first_revision = execution.revision
  iex> execution = Journey.set_value(execution, :name, "Mario")
  iex> execution.revision == first_revision
  true
  ```

  Different value types:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "set workflow - value types example",
  ...>       "v1.0.0",
  ...>       [input(:number), input(:flag), input(:data)]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set_value(execution, :number, 42)
  iex> execution = Journey.set_value(execution, :flag, true)
  iex> execution = Journey.set_value(execution, :data, %{key: "value"})
  iex> Journey.get_value(execution, :number)
  {:ok, 42}
  iex> Journey.get_value(execution, :flag)
  {:ok, true}
  iex> # using execution ID
  iex> updated_execution = Journey.set_value(execution.id, :number, 43)
  iex> Journey.get_value(updated_execution, :number)
  {:ok, 43}
  ```
  """
  def set_value(execution_id, node_name, value)
      when is_binary(execution_id) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value)) do
    execution = Journey.Repo.get!(Execution, execution_id)
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.set_value(execution.id, node_name, value)
  end

  def set_value(execution, node_name, value)
      when is_struct(execution, Execution) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value)) do
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.set_value(execution, node_name, value)
  end

  @doc """
  Removes the value from an input node in an execution and invalidates all dependent computed nodes.

  Automatically invalidates dependent computed nodes through a cascading effect. Ensures data
  consistency - no computed values remain based on removed input.

  ## Quick Example

  ```elixir
  execution = Journey.unset_value(execution, :name)
  {:error, :not_set} = Journey.get_value(execution, :name)
  ```

  ## Parameters
  * `execution` - `%Execution{}` struct or execution ID (string)
  * `node_name` (atom) - Input node name (must exist in graph)

  ## Returns
  * Updated `%Execution{}` with incremented revision (if was set)

  ## Behavior & Errors
  * Cascades to dependent nodes, idempotent, input nodes only
  * Raises `RuntimeError` for compute nodes or unknown nodes

  **See also:** `set_value/3`, `get_value/3`

  ## Examples

  Basic unsetting with cascading invalidation:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "unset workflow - basic example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         compute(
  ...>           :greeting,
  ...>           [:name],
  ...>           fn %{name: name} -> {:ok, "Hello, \#{name}!"} end
  ...>         )
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set_value(execution, :name, "Mario")
  iex> Journey.get_value(execution, :greeting, wait_any: true)
  {:ok, "Hello, Mario!"}
  iex> execution_after_unset = Journey.unset_value(execution, :name)
  iex> Journey.get_value(execution_after_unset, :name)
  {:error, :not_set}
  iex> Journey.get_value(execution_after_unset, :greeting)
  {:error, :not_set}
  ```

  Multi-level cascading (A → B → C chain):

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "unset workflow - cascade example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:a),
  ...>         compute(:b, [:a], fn %{a: a} -> {:ok, "B:\#{a}"} end),
  ...>         compute(:c, [:b], fn %{b: b} -> {:ok, "C:\#{b}"} end)
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set_value(execution, :a, "value")
  iex> Journey.get_value(execution, :b, wait_any: true)
  {:ok, "B:value"}
  iex> Journey.get_value(execution, :c, wait_any: true)
  {:ok, "C:B:value"}
  iex> execution_after_unset = Journey.unset_value(execution, :a)
  iex> Journey.get_value(execution_after_unset, :a)
  {:error, :not_set}
  iex> Journey.get_value(execution_after_unset, :b)
  {:error, :not_set}
  iex> Journey.get_value(execution_after_unset, :c)
  {:error, :not_set}
  ```

  """
  def unset_value(execution_id, node_name)
      when is_binary(execution_id) and is_atom(node_name) do
    execution = Journey.Repo.get!(Execution, execution_id)
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.unset_value(execution, node_name)
  end

  def unset_value(execution, node_name)
      when is_struct(execution, Execution) and is_atom(node_name) do
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.unset_value(execution, node_name)
  end

  @doc """
  Returns the value of a node in an execution. Optionally waits for the value to be set.

  ## Quick Examples

  ```elixir
  # Basic usage - get a set value
  {:ok, value} = Journey.get_value(execution, :name)

  # Wait for a computed value to be available
  {:ok, result} = Journey.get_value(execution, :computed_field, wait_any: true)

  # Wait for a new version of the value
  {:ok, new_value} = Journey.get_value(execution, :name, wait_new: true)
  ```

  Use `set_value/3` to set input values that trigger computations.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct
  * `node_name` - Atom representing the node name (must exist in the graph)
  * `opts` - Keyword list of options (see Options section below)

  ## Returns
  * `{:ok, value}` – if the value is set
  * `{:error, :not_set}` – if the value is not yet set

  ## Errors
  * Raises `RuntimeError` if the node name does not exist in the execution's graph
  * Raises `ArgumentError` if both `:wait_any` and `:wait_new` options are provided (mutually exclusive)

  ## Options
  * `:wait_any` – whether or not to wait for the value to be set. This option can have the following values:
    * `false` or `0` – return immediately without waiting (default)
    * `true` – wait until the value is available, or until timeout
    * a positive integer – wait for the supplied number of milliseconds (default: 30_000)
    * `:infinity` – wait indefinitely
    This is useful for self-computing nodes, where the value is computed asynchronously.
  * `:wait_new` – whether to wait for a new revision of the value, compared to the version in the supplied execution. This option can have the following values:
    * `false` – do not wait for a new revision (default)
    * `true` – wait for a value with a higher revision than the current one, or the first value if none exists yet, or until timeout
    * a positive integer – wait for the supplied number of milliseconds for a new revision
    This is useful for when want a new version of the value, and are waiting for it to get computed.

  **Note:** `:wait_any` and `:wait_new` are mutually exclusive.

  ## Examples

    ```elixir
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
    iex> Journey.get_value(execution, :astrological_sign, wait_any: true)
    {:ok, "Taurus"}
    iex> Journey.get_value(execution, :horoscope, wait_any: 2_000)
    {:error, :not_set}
    iex> execution = Journey.set_value(execution, :first_name, "Mario")
    iex> Journey.get_value(execution, :horoscope, wait_any: true)
    {:ok, "🍪s await, Taurus Mario!"}
    ```

  """
  def get_value(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    check_options(opts, [:wait_any, :wait_new])

    wait_new = Keyword.get(opts, :wait_new, false)
    wait_any = Keyword.get(opts, :wait_any, false)

    # Check for mutually exclusive options
    if wait_new != false and wait_any != false do
      raise ArgumentError, "Options :wait_any and :wait_new are mutually exclusive"
    end

    timeout_ms_or_infinity = determine_timeout(wait_new, wait_any)

    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_node_name(execution, node_name)
    Executions.get_value(execution, node_name, timeout_ms_or_infinity, wait_new: wait_new != false)
  end

  @doc """
  Archives an execution, making it invisible and stopping all background processing.

  Marks with timestamp, excludes from scheduler, preserves data. Reversible via `unarchive/1`.

  ## Quick Example

  ```elixir
  archived_at = Journey.archive(execution)
  Journey.load(execution)  # Returns nil (hidden)
  Journey.load(execution, include_archived: true)  # Can still access
  ```

  ## Parameters
  * `execution` - `%Execution{}` struct or execution ID (string)

  ## Returns
  * Integer timestamp (Unix epoch seconds) when archived

  ## Behavior
  * Excluded from background sweeps and processing
  * Hidden from `list_executions/1` and `load/2` by default
  * Idempotent - returns existing timestamp if already archived
  * Reversible via `unarchive/1`

  **See also:** `unarchive/1`, `list_executions/1`

  ## Examples

  Basic archiving workflow:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph("archive example", "v1.0.0", [input(:data)])
  iex> execution = Journey.start_execution(graph)
  iex> execution.archived_at
  nil
  iex> archived_at = Journey.archive(execution)
  iex> is_integer(archived_at)
  true
  iex> Journey.load(execution)
  nil
  iex> Journey.load(execution, include_archived: true) != nil
  true
  ```

  """
  def archive(execution_id) when is_binary(execution_id) do
    Journey.Executions.archive_execution(execution_id)
  end

  def archive(execution) when is_struct(execution, Journey.Persistence.Schema.Execution),
    do: Journey.archive(execution.id)

  @doc """
  Un-archives the supplied execution, if it is archived.

  ## Parameters
  * `execution` - `%Execution{}` struct or execution ID (string)

  ## Returns
  * `:ok`

  **See also:** `archive/1`

  ## Examples

    ```elixir
    iex> execution =
    ...>    Journey.Examples.Horoscope.graph() |>
    ...>    Journey.start_execution() |>
    ...>    Journey.set_value(:birth_day, 26)
    iex> _archived_at = Journey.archive(execution)
    iex> # The execution is now archived, and it is no longer visible.
    iex> nil == Journey.load(execution, include_archived: false)
    true
    iex> Journey.unarchive(execution)
    :ok
    iex> # The execution is now un-archived, and it can now be loaded.
    iex> nil == Journey.load(execution, include_archived: false)
    false
    iex> # Un-archiving an un-archived execution has no effect.
    iex> Journey.unarchive(execution)
    :ok
    ```
  """
  def unarchive(execution_id) when is_binary(execution_id) do
    Journey.Executions.unarchive_execution(execution_id)
  end

  def unarchive(execution) when is_struct(execution, Journey.Persistence.Schema.Execution),
    do: Journey.unarchive(execution.id)

  @doc false
  def kick(execution_id) when is_binary(execution_id) do
    execution_id
    |> Journey.load()
    |> Journey.Scheduler.advance()
  end

  @default_timeout_ms 30_000

  defp determine_timeout(false, false), do: nil
  defp determine_timeout(wait_new, false), do: timeout_value(wait_new)
  defp determine_timeout(false, wait_any), do: timeout_value(wait_any)

  defp determine_timeout(wait_new, wait_any) do
    raise ArgumentError,
          "Invalid timeout options: wait_new: #{inspect(wait_new)}, wait_any: #{inspect(wait_any)}. " <>
            "Valid values: false, 0, true (in which case the default is #{@default_timeout_ms}), :infinity, or positive integer (milliseconds). " <>
            "Options :wait_any and :wait_new are mutually exclusive."
  end

  defp timeout_value(v) when is_integer(v) or v == :infinity, do: v
  defp timeout_value(true), do: @default_timeout_ms

  # Validates that only known option keys are provided
  defp check_options(supplied_option_names_kwl, known_option_names_list) do
    supplied_option_names = MapSet.new(Keyword.keys(supplied_option_names_kwl))
    known_option_names = MapSet.new(known_option_names_list)

    unexpected_option_names = MapSet.difference(supplied_option_names, known_option_names)

    if unexpected_option_names != MapSet.new([]) do
      raise ArgumentError,
            "Unknown options: #{inspect(MapSet.to_list(unexpected_option_names))}. Known options: #{inspect(MapSet.to_list(known_option_names) |> Enum.sort())}."
    end
  end
end
