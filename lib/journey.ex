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
  iex> e = Journey.set(e, :birth_day, 26)
  iex>
  iex> # As a side note: if the user leaves and comes back later or if everything crashes,
  iex> # you can always reload the execution using its id:
  iex> e = Journey.load(e.id)
  iex>
  iex> # Continuing, as if nothing happened:
  iex> e = Journey.set(e, :birth_month, "April")
  iex>
  iex> # 4. Now that we have :birth_month and :birth_day, :zodiac_sign will compute itself:
  iex> {:ok, "Taurus", _revision} = Journey.get(e, :zodiac_sign, wait: :any)
  iex> Journey.values(e) |> redact([:execution_id, :last_updated_at])
  %{birth_day: 26, birth_month: "April", zodiac_sign: "Taurus", execution_id: "...", last_updated_at: 1234567890}
  iex>
  iex> # 5. Once we get :first_name, the :horoscope node will compute itself:
  iex> e = Journey.set(e, :first_name, "Mario")
  iex> Journey.get(e, :horoscope, wait: :any)
  {:ok, "🍪s await, Taurus Mario!", 7}
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

  @default_timeout_ms 30_000

  @doc group: "Graph Management"
  @doc """
  Creates a new computation graph with the given name, version, and node definitions.

  This is the foundational function for defining Journey graphs. It creates a validated
  graph structure that can be used to start executions with `start_execution/1`. The graph
  defines the data flow, dependencies, and computations for your application workflow.

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

  Use `start_execution/1` to create executions and `set/3` to populate input values.

  ## Parameters
  * `name` - String identifying the graph (e.g., "user registration workflow")
  * `version` - String version identifier following semantic versioning (e.g., "v1.0.0")
  * `nodes` - List of node definitions created with `Journey.Node` functions (`input/1`, `compute/4`, etc.)
  * `opts` - Optional keyword list of options:
    * `:f_on_save` - Graph-wide callback function invoked after any node computation succeeds.
      Receives `(execution_id, node_name, result)` where result is `{:ok, value}` or `{:error, reason}`.
      This callback is called after any node-specific `f_on_save` callbacks.

  ## Returns
  * `%Journey.Graph{}` struct representing the validated and registered computation graph

  ## Errors
  * Raises `RuntimeError` if graph validation fails (e.g., circular dependencies, unknown node references)
  * Raises `ArgumentError` if parameters have invalid types or empty node list
  * Raises `KeywordValidator.Error` if options are invalid

  ## Key Behaviors
  * **Validation** - Automatically validates graph structure for cycles, dependency correctness
  * **Registration** - Registers graph in catalog for execution tracking and reloading
  * **Immutable** - Graph definition is immutable once created; create new versions for changes
  * **Node types** - Supports input, compute, mutate, schedule_once, and schedule_recurring nodes
  * **`f_on_save` Callbacks** - If defined, the graph-wide `f_on_save` callback is called after Node-specific `f_on_save`s (if defined)

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
  iex> execution = Journey.set(execution, :name, "Alice")
  iex> Journey.get(execution, :greeting, wait: :any)
  {:ok, "Hello, Alice!", 3}
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
  iex> execution = Journey.set(execution, :birth_day, 15)
  iex> execution = Journey.set(execution, :birth_month, "May")
  iex> {:ok, "Taurus", _revision} = Journey.get(execution, :zodiac_sign, wait: :any)
  iex> execution = Journey.set(execution, :first_name, "Bob")
  iex> Journey.get(execution, :horoscope, wait: :any)
  {:ok, "🍪s await, Taurus Bob!", 7}
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
  iex> execution = Journey.set(execution, :raw_data, "hello world")
  iex> Journey.get(execution, :upper_case, wait: :any)
  {:ok, "HELLO WORLD", 3}
  iex> Journey.get(execution, :suffix, wait: :any)
  {:ok, "HELLO WORLD omg yay", 5}
  ```

  """
  def new_graph(name, version, nodes, opts \\ [])
      when is_binary(name) and is_binary(version) and is_list(nodes) do
    Graph.new(name, version, nodes, opts)
    |> Journey.Graph.Validations.validate()
    |> Graph.Catalog.register()
  end

  @doc group: "Execution Lifecycle"
  @doc """
  Reloads the current state of an execution from the database to get the latest changes.

  Executions can be modified by their background computations, or scheduled events, or other processes setting their values. This function is used to get the latest state of an execution -- as part of normal operations, or when the system starts up, or when the user whose session is being tracked as an execution comes back to the web site and resumes their flow.

  ## Quick Example

  ```elixir
  execution = Journey.set(execution, :name, "Mario")
  execution = Journey.load(execution)  # Get updated state with new revision
  {:ok, greeting, _} = Journey.get(execution, :greeting, wait: :any)
  ```

  Use `set/3` and `get_value/3` to modify and read execution values.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string
  * `opts` - Keyword list of options (see Options section below)

  ## Returns
  * A `%Journey.Persistence.Schema.Execution{}` struct with current database state, or `nil` if not found

  ## Options
  * `:preload` - Whether to preload associated nodes and values. Defaults to `true`.
    Set to `false` for better performance when you only need execution metadata.
  * `:include_archived` - Whether to include archived executions. Defaults to `false`.
    Archived executions are normally hidden but can be loaded with this option.

  ## Key Behaviors
  * **Fresh state** - Always returns the current state from the database, not cached data
  * **Revision tracking** - Loaded execution will have the latest revision number
  * **Archived handling** - Archived executions return `nil` unless explicitly included
  * **Performance option** - Use `preload: false` to skip loading values/computations for speed

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
  iex> execution = Journey.set(execution, :name, "Alice")
  iex> execution.revision > 0
  true
  iex> {:ok, "Hello, Alice!", _} = Journey.get(execution, :greeting, wait: :any)
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

  @doc group: "Execution Lifecycle"
  @doc """
  Queries and retrieves multiple executions from the database with flexible filtering, sorting, and pagination.

  This function enables searching across all executions in your system, with powerful filtering
  capabilities based on graph names, node values, and execution metadata. It's essential for
  monitoring workflows, building dashboards, and analyzing execution patterns.

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

  Use with `start_execution/1` to create executions and `load/2` to get individual execution details.

  ## Parameters
  * `options` - Keyword list of query options (all optional):
    * `:graph_name` - String name of a specific graph to filter by
    * `:graph_version` - String version of a specific graph to filter by (requires :graph_name)
    * `:sort_by` - List of fields to sort by, including both execution fields and node values (see Sorting section for details)
    * `:filter_by` - List of node value filters using database-level filtering for optimal performance. Each filter is a tuple `{node_name, operator, value}` or `{node_name, operator}` for nil checks. Operators: `:eq`, `:neq`, `:lt`, `:lte`, `:gt`, `:gte` (comparisons), `:in`, `:not_in` (membership), `:contains` (case-sensitive substring matching, strings only), `:icontains` (case-insensitive substring matching, strings only), `:list_contains` (checks if a list-valued node contains the specified string or integer element), `:is_nil`, `:is_not_nil` (existence). Values can be strings, numbers, booleans, nil or lists (used with `:in` and `:not_in`). Complex values (maps, tuples, functions) will raise an ArgumentError.
    * `:limit` - Maximum number of results (default: 10,000)
    * `:offset` - Number of results to skip for pagination (default: 0)
    * `:include_archived` - Whether to include archived executions (default: false)

  ## Returns
  * List of `%Journey.Persistence.Schema.Execution{}` structs with preloaded values and computations
  * Empty list `[]` if no executions match the criteria

  ## Options

  ### `:sort_by`
  Sort by execution fields or node values. Supports atoms for ascending (`[:updated_at]`),
  keywords for direction (`[updated_at: :desc]`), and mixed formats (`[:graph_name, inserted_at: :desc]`).

  **Available fields:**
  * Execution fields: `:inserted_at`, `:updated_at`, `:revision`, `:graph_name`, `:graph_version`
  * Node values: Any node name from the graph (e.g., `:age`, `:score`) using JSONB ordering
  * Direction: `:asc` (default) or `:desc`

  ## Key Behaviors
  * Filtering performed at database level for optimal performance
  * Only primitive values supported for filtering (complex types raise errors)
  * Archived executions excluded by default

  ## Examples

  Basic listing by graph name:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "list example basic - \#{Journey.Helpers.Random.random_string_w_time()}",
  ...>   "v1.0.0",
  ...>   [input(:status)]
  ...> )
  iex> Journey.start_execution(graph) |> Journey.set(:status, "active")
  iex> Journey.start_execution(graph) |> Journey.set(:status, "pending")
  iex> executions = Journey.list_executions(graph_name: graph.name)
  iex> length(executions)
  2
  ```

  Filtering by graph version:

  ```elixir
  iex> import Journey.Node
  iex> graph_name = "version example #{Journey.Helpers.Random.random_string_w_time()}"
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
  iex> Journey.start_execution(graph_v1) |> Journey.set(:data, "v1 data")
  iex> Journey.start_execution(graph_v2) |> Journey.set(:data, "v2 data")
  iex> Journey.list_executions(graph_name: graph_v1.name, graph_version: "v1.0.0") |> length()
  1
  iex> Journey.list_executions(graph_name: graph_v1.name, graph_version: "v2.0.0") |> length()
  1
  iex> Journey.list_executions(graph_name: graph_v1.name) |> length()
  2
  ```

  Validation that graph_version requires graph_name:

  ```elixir
  iex> Journey.list_executions(graph_version: "v1.0.0")
  ** (ArgumentError) Option :graph_version requires :graph_name to be specified
  ```

  Sorting by execution fields and node values:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "sort example - \#{Journey.Helpers.Random.random_string_w_time()}",
  ...>   "v1.0.0",
  ...>   [input(:priority)]
  ...> )
  iex> Journey.start_execution(graph) |> Journey.set(:priority, "high")
  iex> Journey.start_execution(graph) |> Journey.set(:priority, "low")
  iex> Journey.start_execution(graph) |> Journey.set(:priority, "medium")
  iex> # Sort by priority descending - shows the actual sorted values
  iex> Journey.list_executions(graph_name: graph.name, sort_by: [priority: :desc]) |> Enum.map(fn e -> Journey.values(e) |> Map.get(:priority) end)
  ["medium", "low", "high"]
  ```

  Filtering with multiple operators:

  ```elixir
  iex> graph = Journey.Examples.Horoscope.graph()
  iex> for day <- 1..20, do: Journey.start_execution(graph) |> Journey.set(:birth_day, day) |> Journey.set(:birth_month, 4) |> Journey.set(:first_name, "Mario")
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
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :contains, "ari"}]) |> Enum.count()
  20
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:first_name, :icontains, "MARIO"}]) |> Enum.count()
  20
  ```

  List containment filtering with `:list_contains`:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "notification example - \#{Journey.Helpers.Random.random_string_w_time()}",
  ...>   "v1.0.0",
  ...>   [input(:recipients)]
  ...> )
  iex> Journey.start_execution(graph) |> Journey.set(:recipients, ["user1", "user2", "admin"])
  iex> Journey.start_execution(graph) |> Journey.set(:recipients, ["user3", "user4"])
  iex> Journey.start_execution(graph) |> Journey.set(:recipients, [1, 2, 3])
  iex> # Find executions where recipients list contains "user1"
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, "user1"}]) |> Enum.count()
  1
  iex> # Find executions where recipients list contains integer 2
  iex> Journey.list_executions(graph_name: graph.name, filter_by: [{:recipients, :list_contains, 2}]) |> Enum.count()
  1
  ```

  Multiple filters, sorting, and pagination:

  ```elixir
  iex> graph = Journey.Examples.Horoscope.graph()
  iex> for day <- 1..20, do: Journey.start_execution(graph) |> Journey.set(:birth_day, day) |> Journey.set(:birth_month, 4) |> Journey.set(:first_name, "Mario")
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
  ...>   "list example - archived - \#{Journey.Helpers.Random.random_string_w_time()}",
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

  @doc group: "Execution Lifecycle"
  @doc """
  Starts a new execution instance of a computation graph, initializing it to accept input values and perform computations.

  Creates a persistent execution in the database with a unique ID and begins background processing
  for any schedulable nodes. The execution starts with revision 0 and no values set.

  ## Quick Example

  ```elixir
  execution = Journey.start_execution(graph)
  execution = Journey.set(execution, :name, "Mario")
  {:ok, greeting, _} = Journey.get(execution, :greeting, wait: :any)
  ```

  Use `set/3` to provide input values and `get_value/3` to retrieve computed results.

  ## Parameters
  * `graph` - A validated `%Journey.Graph{}` struct created with `new_graph/3`. The graph must
    have passed validation during creation and be registered in the graph catalog.

  ## Returns
  * A new `%Journey.Persistence.Schema.Execution{}` struct with:
    * `:id` - Unique execution identifier (UUID string)
    * `:graph_name` and `:graph_version` - From the source graph
    * `:revision` - Always starts at 0, increments with each state change
    * `:archived_at` - Initially nil (not archived)
    and other fields.

  ## Key Behaviors
  * **Database persistence** - Execution state is immediately saved to PostgreSQL
  * **Unique execution** - Each call creates a completely independent execution instance
  * **Background processing** - Scheduler automatically begins monitoring for schedulable nodes
  * **Ready for inputs** - Can immediately accept input values via `set/3`

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
  iex> execution = Journey.set(execution, :x, 10)
  iex> execution = Journey.set(execution, :y, 20)
  iex> {:ok, 30, _revision} = Journey.get(execution, :sum, wait: :any)
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
  iex> execution1 = Journey.set(execution1, :count, 1)
  iex> execution2 = Journey.set(execution2, :count, 2)
  iex> Journey.get(execution1, :count)
  {:ok, 1, 1}
  iex> Journey.get(execution2, :count)
  {:ok, 2, 1}
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

  @doc group: "Data Retrieval"
  @doc """
  Returns a map of all nodes in an execution with their current status, including unset nodes.

  Unlike `values/2` which only returns set nodes, this function shows all nodes including those
  that haven't been set yet. Unset nodes are marked as `:not_set`, while set nodes are returned
  as `{:set, value}` tuples. Useful for debugging and introspection.

  ## Quick Example

  ```elixir
  all_status = Journey.values_all(execution)
  # %{name: {:set, "Alice"}, age: :not_set, execution_id: {:set, "EXEC..."}, ...}
  ```

  Use `values/2` to get only set values, or `get_value/3` for individual node values.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct
  * `opts` - Keyword list of options (`:reload` - defaults to `true` for fresh database state)

  ## Returns
  * Map with all nodes showing status: `:not_set` or `{:set, value}`
  * Includes all nodes defined in the graph, regardless of current state

  ## Examples

  Basic usage showing status progression:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph("example", "v1.0.0", [input(:name), input(:age)])
  iex> execution = Journey.start_execution(graph)
  iex> Journey.values_all(execution) |> redact([:execution_id, :last_updated_at])
  %{name: :not_set, age: :not_set, execution_id: {:set, "..."}, last_updated_at: {:set, 1234567890}}
  iex> execution = Journey.set(execution, :name, "Alice")
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

  @doc group: "Data Retrieval"
  @doc """
  Returns a map of all set node values in an execution, excluding unset nodes.

  This function filters the execution to only include nodes that have been populated with data.
  Unset nodes are excluded from the result. Always includes `:execution_id` and `:last_updated_at` metadata.

  ## Quick Example

  ```elixir
  execution = Journey.set(execution, :name, "Alice")
  values = Journey.values(execution)
  # %{name: "Alice", execution_id: "EXEC...", last_updated_at: 1234567890}
  ```

  Use `values_all/1` to see all nodes including unset ones, or `get_value/3` for individual values.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct
  * `opts` - Keyword list of options (`:reload` - see `values_all/1` for details)

  ## Returns
  * Map with node names as keys and their current values as values
  * Only includes nodes that have been set (excludes `:not_set` nodes)

  ## Examples

  Basic usage:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph("example", "v1.0.0", [input(:name), input(:age)])
  iex> execution = Journey.start_execution(graph)
  iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
  %{execution_id: "...", last_updated_at: 1234567890}
  iex> execution = Journey.set(execution, :name, "Alice")
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

  @doc group: "Execution Lifecycle"
  @doc """
  Returns the chronological history of all successful computations and set values for an execution.

  This function provides visibility into the order of operations during execution, showing both
  value sets and successful computations in chronological order. Only successful computations
  are included; failed computations are filtered out. At the same revision, computations appear
  before values.

  ## Quick Example

  ```elixir
  history = Journey.history(execution)
  # [%{node_name: :x, computation_or_value: :value, revision: 1},
  #  %{node_name: :sum, computation_or_value: :computation, revision: 2}, ...]
  ```

  Use `values/2` to see only current values, or `set/3` and `get_value/3` for individual operations.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string

  ## Returns
  * List of maps sorted by revision, where each map contains:
    * `:computation_or_value` - either `:computation` or `:value`
    * `:node_name` - the name of the node
    * `:node_type` - the type of the node (`:input`, `:compute`, `:mutate`, etc.)
    * `:revision` - the execution revision when this operation completed
    * `:value` - the actual value (only present for `:value` entries)

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
  iex> execution = Journey.set(execution, :x, 10)
  iex> execution = Journey.set(execution, :y, 20)
  iex> {:ok, 30, _revision} = Journey.get(execution, :sum, wait: :any)
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

  @doc group: "Value Operations"
  @doc """
  Sets values for input nodes in an execution and triggers recomputation of dependent nodes.

  This function supports three calling patterns:
  1. Single value: `set(execution, :node_name, value)`
  2. Multiple values via map: `set(execution, %{node1: value1, node2: value2})`
  3. Multiple values via keyword list: `set(execution, node1: value1, node2: value2)`

  When values are set, Journey automatically recomputes any dependent computed nodes to ensure
  consistency across the dependency graph. The operation is idempotent - setting the same values
  has no effect.

  ## Parameters

  **Single value:**
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string
  * `node_name` - Atom representing the input node name (must exist in the graph)
  * `value` - The value to set. Supported types: nil, string, number, map, list, boolean. Note that if the map or the list contains atoms, those atoms will be converted to strings.

  **Multiple values:**
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string
  * `values` - Map of node names to values (e.g., `%{node1: "value1", node2: 42}`) or keyword list (e.g., `[node1: "value1", node2: 42]`)

  ## Returns
  * Updated `%Journey.Persistence.Schema.Execution{}` struct with incremented revision (if any value changed)

  ## Errors
  * Raises `RuntimeError` if any node name does not exist in the execution's graph
  * Raises `RuntimeError` if attempting to set compute nodes (only input nodes can be set)

  ## Key Behaviors
  * **Automatic recomputation** - Setting values triggers recomputation of all dependent nodes
  * **Idempotent** - Setting the same values has no effect (no revision increment)
  * **Input nodes only** - Only input nodes can be set; compute nodes are read-only
  * **Atomic updates** - Multiple values are set together in a single transaction (single revision increment)

  ## Quick Examples

  ```elixir
  # Single value
  execution = Journey.set(execution, :name, "Mario")

  # Multiple values via map
  execution = Journey.set(execution, %{name: "Mario", age: 35})

  # Multiple values via keyword list
  execution = Journey.set(execution, name: "Mario", age: 35)

  {:ok, greeting, _} = Journey.get(execution, :greeting, wait: :any)
  ```

  Use `get_value/3` to retrieve values and `unset/2` to remove values.

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
  iex> execution = Journey.set(execution, :name, "Mario")
  iex> Journey.get(execution, :greeting, wait: :any)
  {:ok, "Hello, Mario!", 3}
  iex> execution = Journey.set(execution, :name, "Luigi")
  iex> {:ok, "Hello, Luigi!", _revision} = Journey.get(execution, :greeting, wait: :newer)
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
  iex> execution = Journey.set(execution, :name, "Mario")
  iex> first_revision = execution.revision
  iex> execution = Journey.set(execution, :name, "Mario")
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
  iex> execution = Journey.set(execution, :number, 42)
  iex> execution = Journey.set(execution, :flag, true)
  iex> execution = Journey.set(execution, :data, %{key: "value"})
  iex> {:ok, 42, _revision} = Journey.get(execution, :number)
  iex> {:ok, true, _revision} = Journey.get(execution, :flag)
  ```

  Using an execution ID:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "set workflow - execution_id example",
  ...>       "v1.0.0",
  ...>       [input(:name)]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> updated_execution = Journey.set(execution.id, :name, "Luigi")
  iex> {:ok, "Luigi", _revision} = Journey.get(updated_execution, :name)
  ```

  Multiple values via map (atomic operation):

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "set workflow - multiple map example",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:first_name),
  ...>         input(:last_name),
  ...>         compute(:full_name, [:first_name, :last_name], fn %{first_name: first, last_name: last} ->
  ...>           {:ok, "\#{first} \#{last}"}
  ...>         end)
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set(execution, %{first_name: "Mario", last_name: "Bros"})
  iex> Journey.get(execution, :first_name)
  {:ok, "Mario", 1}
  iex> Journey.get(execution, :last_name)
  {:ok, "Bros", 1}
  iex> Journey.get(execution, :full_name, wait: :any)
  {:ok, "Mario Bros", 3}
  ```

  Multiple values via keyword list (ergonomic syntax):

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "set workflow - keyword example",
  ...>       "v1.0.0",
  ...>       [input(:name), input(:age), input(:active)]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set(execution, name: "Mario", age: 35, active: true)
  iex> Journey.get(execution, :name)
  {:ok, "Mario", 1}
  iex> Journey.get(execution, :age)
  {:ok, 35, 1}
  iex> Journey.get(execution, :active)
  {:ok, true, 1}
  ```

  """
  def set(execution_id, node_name, value)
      when is_binary(execution_id) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value)) do
    execution = Journey.Repo.get!(Execution, execution_id)
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.set_value(execution.id, node_name, value)
  end

  @doc group: "Value Operations"
  def set(execution, node_name, value)
      when is_struct(execution, Execution) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value)) do
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.set_value(execution, node_name, value)
  end

  @doc group: "Value Operations"
  def set(execution_id, values_map)
      when is_binary(execution_id) and is_map(values_map) do
    execution = Journey.load(execution_id)
    set(execution, values_map)
  end

  @doc group: "Value Operations"
  def set(execution, values_map)
      when is_struct(execution, Execution) and is_map(values_map) do
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)

    # Validate all node names and values first
    validate_values_map(execution, values_map)

    Journey.Executions.set_values(execution, values_map)
  end

  # Multiple values via keyword list - converts to map
  @doc group: "Value Operations"
  def set(execution, keyword_list)
      when (is_struct(execution, Execution) or is_binary(execution)) and is_list(keyword_list) and keyword_list != [] do
    # Ensure it's a proper keyword list
    if Keyword.keyword?(keyword_list) do
      set(execution, Map.new(keyword_list))
    else
      # If it's not a keyword list, it might be a list value for a single node
      # This case should fall through to the function clause error
      raise FunctionClauseError, message: "Expected keyword list for multiple values or valid single value arguments"
    end
  end

  # Deprecated aliases for backward compatibility
  @doc group: "Deprecated"
  @deprecated "Use Journey.set/3 instead"
  def set_value(execution_id, node_name, value)
      when is_binary(execution_id) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value)) do
    set(execution_id, node_name, value)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.set/3 instead"
  def set_value(execution, node_name, value)
      when is_struct(execution, Execution) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value)) do
    set(execution, node_name, value)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.set/2 instead"
  def set_value(execution_id, values_map)
      when is_binary(execution_id) and is_map(values_map) do
    set(execution_id, values_map)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.set/2 instead"
  def set_value(execution, values_map)
      when is_struct(execution, Execution) and is_map(values_map) do
    set(execution, values_map)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.set/2 instead"
  def set_value(execution, keyword_list)
      when (is_struct(execution, Execution) or is_binary(execution)) and is_list(keyword_list) and keyword_list != [] do
    set(execution, keyword_list)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.unset/2 instead"
  def unset_value(execution_id, node_name)
      when is_binary(execution_id) and is_atom(node_name) do
    unset(execution_id, node_name)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.unset/2 instead"
  def unset_value(execution, node_name)
      when is_struct(execution, Execution) and is_atom(node_name) do
    unset(execution, node_name)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.unset/2 instead"
  def unset_value(execution_id, node_names)
      when is_binary(execution_id) and is_list(node_names) and node_names != [] do
    unset(execution_id, node_names)
  end

  @doc group: "Deprecated"
  @deprecated "Use Journey.unset/2 instead"
  def unset_value(execution, node_names)
      when is_struct(execution, Execution) and is_list(node_names) and node_names != [] do
    unset(execution, node_names)
  end

  @doc group: "Value Operations"
  @doc """
  Removes values from input nodes in an execution and invalidates all dependent computed nodes.

  This function supports two calling patterns:
  1. Single value: `unset(execution, :node_name)`
  2. Multiple values via list: `unset(execution, [:node1, :node2, :node3])`

  When values are unset, Journey automatically invalidates (unsets) all computed nodes that depend
  on the unset inputs, creating a cascading effect through the dependency graph. This ensures data
  consistency - no computed values remain that were based on the now-removed inputs.

  ## Quick Examples

  ```elixir
  # Single value
  execution = Journey.unset(execution, :name)
  {:error, :not_set} = Journey.get(execution, :name)

  # Multiple values
  execution = Journey.unset(execution, [:first_name, :last_name, :email])
  ```

  Use `set/3` to set values and `get_value/3` to check if values are set.

  ## Parameters

  **Single value:**
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string
  * `node_name` - Atom representing the input node name (must exist in the graph)

  **Multiple values:**
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string
  * `node_names` - List of atoms representing input node names (all must exist in the graph)

  ## Returns
  * Updated `%Journey.Persistence.Schema.Execution{}` struct with incremented revision (if value was set)

  ## Errors
  * Raises `RuntimeError` if the node name does not exist in the execution's graph
  * Raises `RuntimeError` if attempting to unset a compute node (only input nodes can be unset)

  ## Key Behaviors
  * **Cascading invalidation** - Dependent computed nodes are automatically unset
  * **Idempotent** - Multiple unsets of the same value have no additional effect
  * **Input nodes only** - Only input nodes can be unset; compute nodes cannot be unset
  * **Atomic updates** - Multiple values are unset together in a single transaction (single revision increment)

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
  iex> execution = Journey.set(execution, :name, "Mario")
  iex> Journey.get(execution, :greeting, wait: :any)
  {:ok, "Hello, Mario!", 3}
  iex> execution_after_unset = Journey.unset(execution, :name)
  iex> Journey.get(execution_after_unset, :name)
  {:error, :not_set}
  iex> Journey.get(execution_after_unset, :greeting)
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
  iex> execution = Journey.set(execution, :a, "value")
  iex> Journey.get(execution, :b, wait: :any)
  {:ok, "B:value", 3}
  iex> Journey.get(execution, :c, wait: :any)
  {:ok, "C:B:value", 5}
  iex> execution_after_unset = Journey.unset(execution, :a)
  iex> Journey.get(execution_after_unset, :a)
  {:error, :not_set}
  iex> Journey.get(execution_after_unset, :b)
  {:error, :not_set}
  iex> Journey.get(execution_after_unset, :c)
  {:error, :not_set}
  ```

  Idempotent behavior:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "unset workflow - idempotent example",
  ...>   "v1.0.0",
  ...>   [input(:name)]
  ...> )
  iex> execution = graph |> Journey.start_execution()
  iex> original_revision = execution.revision
  iex> execution_after_unset = Journey.unset(execution, :name)
  iex> execution_after_unset.revision == original_revision
  true
  ```

  Multiple values atomic operation:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "unset workflow - multiple values example",
  ...>   "v1.0.0",
  ...>   [
  ...>     input(:first_name),
  ...>     input(:last_name),
  ...>     input(:email),
  ...>     compute(:full_name, [:first_name, :last_name], fn %{first_name: first_name, last_name: last_name} ->
  ...>       {:ok, "\#{first_name} \#{last_name}"}
  ...>     end)
  ...>   ]
  ...> )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set(execution, %{first_name: "Mario", last_name: "Bros", email: "mario@example.com"})
  iex> Journey.get(execution, :full_name, wait: :any)
  {:ok, "Mario Bros", 3}
  iex> execution_after_unset = Journey.unset(execution, [:first_name, :last_name])
  iex> Journey.get(execution_after_unset, :first_name)
  {:error, :not_set}
  iex> Journey.get(execution_after_unset, :last_name)
  {:error, :not_set}
  iex> Journey.get(execution_after_unset, :email)
  {:ok, "mario@example.com", 1}
  iex> Journey.get(execution_after_unset, :full_name)
  {:error, :not_set}
  ```

  """
  def unset(execution_id, node_name)
      when is_binary(execution_id) and is_atom(node_name) do
    execution = Journey.Repo.get!(Execution, execution_id)
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.unset_value(execution, node_name)
  end

  def unset(execution, node_name)
      when is_struct(execution, Execution) and is_atom(node_name) do
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.unset_value(execution, node_name)
  end

  # Multiple values via list
  def unset(execution_id, node_names)
      when is_binary(execution_id) and is_list(node_names) and node_names != [] do
    execution = Journey.load(execution_id)
    unset(execution, node_names)
  end

  def unset(execution, node_names)
      when is_struct(execution, Execution) and is_list(node_names) and node_names != [] do
    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)

    # Validate all node names are valid input nodes
    for node_name <- node_names do
      unless is_atom(node_name), do: raise(ArgumentError, "All node names must be atoms, got: #{inspect(node_name)}")
      Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    end

    Journey.Executions.unset_values(execution, node_names)
  end

  @doc group: "Value Operations"
  @doc """
  Returns the value and revision of a node in an execution. Optionally waits for the value to be set.

  This function atomically returns both the node value and its revision number, eliminating
  race conditions when you need to track which revision of a value you received.

  ## Quick Examples

  ```elixir
  # Basic usage - get a set value and its revision immediately
  {:ok, value, revision} = Journey.get(execution, :name)

  # Wait for a computed value to be available (30 second default timeout)
  {:ok, result, revision} = Journey.get(execution, :computed_field, wait: :any)

  # Wait for a new version of the value with custom timeout
  {:ok, new_value, new_revision} = Journey.get(execution, :name, wait: :newer, timeout: 5000)

  # Wait for a value newer than a specific revision
  {:ok, fresh_value, fresh_revision} = Journey.get(execution, :name, wait: {:newer_than, 10})
  ```

  Use `set/3` to set input values that trigger computations.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct
  * `node_name` - Atom representing the node name (must exist in the graph)
  * `opts` - Keyword list of options (see Options section below)

  ## Returns
  * `{:ok, value, revision}` – the value is set, with its revision number
  * `{:error, :not_set}` – the value is not yet set
  * `{:error, :computation_failed}` – the computation permanently failed

  ## Errors
  * Raises `RuntimeError` if the node name does not exist in the execution's graph
  * Raises `ArgumentError` if an invalid `:wait` option is provided

  ## Options
  * `:wait` – Controls waiting behavior:
    * `:immediate` (default) – Return immediately without waiting
    * `:any` – Wait until the value is available or timeout
    * `:newer` – Wait for a newer revision than current execution
    * `{:newer_than, revision}` – Wait for value newer than specific revision
  * `:timeout` – Timeout in milliseconds (default: 30,000) or `:infinity`

  ## Examples
    ```elixir
    iex> execution =
    ...>    Journey.Examples.Horoscope.graph() |>
    ...>    Journey.start_execution() |>
    ...>    Journey.set(:birth_day, 26)
    iex> {:ok, 26, _revision} = Journey.get(execution, :birth_day)
    iex> Journey.get(execution, :birth_month)
    {:error, :not_set}
    iex> Journey.get(execution, :astrological_sign)
    {:error, :not_set}
    iex> execution = Journey.set(execution, :birth_month, "April")
    iex> Journey.get(execution, :astrological_sign)
    {:error, :not_set}
    iex> {:ok, "Taurus", _revision} = Journey.get(execution, :astrological_sign, wait: :any)
    iex> Journey.get(execution, :horoscope, wait: :any, timeout: 2_000)
    {:error, :not_set}
    iex> execution = Journey.set(execution, :first_name, "Mario")
    iex> {:ok, "🍪s await, Taurus Mario!", _revision} = Journey.get(execution, :horoscope, wait: :any)
    ```

  """
  def get(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    check_options(opts, [:wait, :timeout])
    wait = Keyword.get(opts, :wait, :immediate)
    timeout = Keyword.get(opts, :timeout, @default_timeout_ms)

    # Parse wait option and determine internal parameters
    {timeout_ms_or_infinity, wait_new_flag, wait_for_revision} = parse_wait_option(wait, timeout, execution)

    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_node_name(execution, node_name)

    # Call internal function with parsed options
    internal_opts = []
    internal_opts = if wait_new_flag, do: Keyword.put(internal_opts, :wait_new, true), else: internal_opts

    internal_opts =
      if wait_for_revision != nil,
        do: Keyword.put(internal_opts, :wait_for_revision, wait_for_revision),
        else: internal_opts

    result = Executions.get_value_node(execution, node_name, timeout_ms_or_infinity, internal_opts)

    case result do
      {:ok, value_node} -> {:ok, value_node.node_value, value_node.ex_revision}
      error -> error
    end
  end

  def get_value(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    # Check for new vs old style options
    has_new_style = Keyword.has_key?(opts, :wait) or Keyword.has_key?(opts, :timeout)
    has_old_style = Keyword.has_key?(opts, :wait_any) or Keyword.has_key?(opts, :wait_new)

    if has_new_style and has_old_style do
      raise ArgumentError,
            "Cannot mix new style options (:wait, :timeout) with old style options (:wait_any, :wait_new)"
    end

    if has_new_style do
      handle_new_style_options(execution, node_name, opts)
    else
      handle_old_style_options(execution, node_name, opts)
    end
  end

  # Handle new style options: wait: and timeout:
  defp handle_new_style_options(execution, node_name, opts) do
    check_options(opts, [:wait, :timeout])

    wait = Keyword.get(opts, :wait, :immediate)
    timeout = Keyword.get(opts, :timeout, @default_timeout_ms)

    # Parse wait option and determine internal parameters
    {timeout_ms_or_infinity, wait_new_flag, wait_for_revision} = parse_wait_option(wait, timeout, execution)

    execution = Journey.Executions.migrate_to_current_graph_if_needed(execution)
    Journey.Graph.Validations.ensure_known_node_name(execution, node_name)

    # Call internal function with parsed options
    internal_opts = []
    internal_opts = if wait_new_flag, do: Keyword.put(internal_opts, :wait_new, true), else: internal_opts

    internal_opts =
      if wait_for_revision != nil,
        do: Keyword.put(internal_opts, :wait_for_revision, wait_for_revision),
        else: internal_opts

    Executions.get_value(execution, node_name, timeout_ms_or_infinity, internal_opts)
  end

  # Handle old style options: wait_any: and wait_new: (backwards compatibility)
  defp handle_old_style_options(execution, node_name, opts) do
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

  # Parse the new :wait option into internal representation
  defp parse_wait_option(:immediate, _timeout, _execution) do
    {nil, false, nil}
  end

  defp parse_wait_option(:any, timeout, _execution) do
    {validate_timeout(timeout), false, nil}
  end

  defp parse_wait_option(:newer, timeout, _execution) do
    {validate_timeout(timeout), true, nil}
  end

  defp parse_wait_option({:newer_than, revision}, timeout, _execution) when is_integer(revision) do
    {validate_timeout(timeout), true, revision}
  end

  defp parse_wait_option(invalid_wait, _timeout, _execution) do
    raise ArgumentError,
          "Invalid :wait option: #{inspect(invalid_wait)}. Valid options: :immediate, :any, :newer, {:newer_than, revision}"
  end

  # Validate timeout values
  defp validate_timeout(timeout) when is_integer(timeout) and timeout > 0, do: timeout
  defp validate_timeout(:infinity), do: :infinity

  defp validate_timeout(timeout) do
    raise ArgumentError, "Invalid timeout value: #{inspect(timeout)}. Must be a positive integer or :infinity"
  end

  @doc group: "Execution Lifecycle"
  @doc """
  Archives an execution, making it invisible and stopping all background processing.

  Archiving permanently (*) freezes an execution by marking it with an archived timestamp.
  This removes it from normal visibility and excludes it from all scheduler processing,
  while preserving the data for potential future access.

  *) an execution can be unarchived by calling `unarchive/1`

  ## Quick Example

  ```elixir
  archived_at = Journey.archive(execution)
  Journey.load(execution)  # Returns nil (hidden)
  Journey.load(execution, include_archived: true)  # Can still access
  ```

  Use `unarchive/1` to reverse archiving and `list_executions/1` with `:include_archived` to find archived executions.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string

  ## Returns
  * Integer timestamp (Unix epoch seconds) when the execution was archived

  ## Key Behaviors
  * **Scheduler exclusion** - Archived executions are excluded from all background sweeps and processing
  * **Hidden by default** - Not returned by `list_executions/1` or `load/2` unless explicitly included
  * **Idempotent** - Archiving an already archived execution returns the existing timestamp
  * **Reversible** - Use `unarchive/1` to restore normal visibility and processing

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

  Idempotent behavior:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph("archive idempotent", "v1.0.0", [input(:data)])
  iex> execution = Journey.start_execution(graph)
  iex> first_archive = Journey.archive(execution)
  iex> second_archive = Journey.archive(execution)
  iex> first_archive == second_archive
  true
  ```

  """
  def archive(execution_id) when is_binary(execution_id) do
    Journey.Executions.archive_execution(execution_id)
  end

  def archive(execution) when is_struct(execution, Journey.Persistence.Schema.Execution),
    do: Journey.archive(execution.id)

  @doc group: "Execution Lifecycle"
  @doc """
  Un-archives the supplied execution, if it is archived.

  ## Parameters:
  - `execution` or `execution_id`: The execution to un-archive, or the ID of the execution to un-archive.

  Returns
  * :ok

  ## Examples

    ```elixir
    iex> execution =
    ...>    Journey.Examples.Horoscope.graph() |>
    ...>    Journey.start_execution() |>
    ...>    Journey.set(:birth_day, 26)
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

  defp validate_values_map(execution, values_map) do
    Enum.each(values_map, fn {node_name, value} ->
      validate_node_name(node_name)
      validate_value_type(node_name, value)
      Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    end)
  end

  defp validate_node_name(node_name) do
    unless is_atom(node_name) do
      raise ArgumentError, "Node names must be atoms, got: #{inspect(node_name)}"
    end
  end

  defp validate_value_type(node_name, value) do
    unless value == nil or is_binary(value) or is_number(value) or is_map(value) or
             is_list(value) or is_boolean(value) do
      raise ArgumentError, "Invalid value type for node #{node_name}: #{inspect(value)}"
    end
  end
end
