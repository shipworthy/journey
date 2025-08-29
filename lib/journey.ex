defmodule Journey do
  @moduledoc """
  This module provides functions for building and executing computation graphs, with persistence, reliability, and scalability.

  ## TL;DR

  Journey is a library for building and executing computation graphs.

  It lets you define your application as a self-computing graph and run it without having to worry about the nitty-gritty of persistence, dependencies, scalability, or reliability.

  Here is a basic example of using Journey. The example's graph defines values, the computation, and dependencies. Once `:x` and `:y` are provided or updated, `:sum` gets computed or recomputed.
  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "demo graph",
  ...>   "v1",
  ...>   [
  ...>     input(:x),
  ...>     input(:y),
  ...>     # the `:sum` computation requires :x and :y.
  ...>     compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end)
  ...>   ]
  ...> )
  iex> execution = Journey.start_execution(graph)
  iex> execution = Journey.set_value(execution, :x, 12)
  iex> execution = Journey.set_value(execution, :y, 2)
  iex> Journey.get_value(execution, :sum, wait_any: true)
  {:ok, 14}
  iex> execution = Journey.set_value(execution, :y, 7)
  iex> Journey.get_value(execution, :sum, wait_new: true)
  {:ok, 19}

  ```

  A few things to note about this example:
  * every input value (:x, :y), or computation result (:sum) is persisted,
  * the :sum computation happens reliably (with a retry policy),
  * the :sum computation is as horizontally distributed as your app,
  * the :sum computation is proactive: it will be computed when x and y become available,
  * the executions of this flow can take as long as needed (milliseconds? months?), and will live through system restarts, crashes, redeployments, page reloads, etc.

  You can see a livebook with this example in [basic.livemd](basic.html)


  ## So What Exactly Does Journey Provide?

  Despite the simplicity of use, here are a few things provided by Journey that are worth noting:

  * Persistence: Executions are persisted, so if the customer leaves the web site, or if the system crashes, their execution can be reloaded and continued from where it left off.

  * Scaling: Since Journey runs as part of your application, it scales with your application. Your graph's computations (`:sum`'s function in the example above, or `&compute_zodiac_sign/1` and `&compute_horoscope/1` in the example above) run on the same nodes where the replicas of your application are running. No additional infrastructure or cloud services are needed.

  * Reliability: Journey uses database-based supervision of computation tasks: The `compute` functions are subject to customizable retry policy, so if `:sum`'s function above or `&compute_horoscope/1` below fails because of a temporary glitch (e.g. the LLM service it uses for drafting horoscopes is currently overloaded), it will be retried.

  * Code Structure: The flow of your application is captured in the Journey graph, and the business logic is captured in the compute functions (`:sum`'s function above, or `&compute_zodiac_sign/1` and `&compute_horoscope/1` below). This clean separation supports you in structuring the functionality of your application in a clear, easy to understand and maintain way.

  * Conditional flow: Journey allows you to define conditions for when a node is to be unblocked. So if your graph includes a "credit_approval_decision" node, the decision can inform which part of the graph is to be executed next (sending a "congrats!" email and starting the credit card issuance process, or sending a "sad trombone" email).

  * Graph Visualization: Journey provides tools for visualizing your application's graph, so you can easily see the flow of data and computations in your application, and to share and discuss it with your team.

  * Scheduling: Your graph can include computations that are scheduled to run at a later time, or on a recurring basis. Daily horoscope emails! A reminder email if they haven't visited the web site in a while! A "happy birthday" email!

  * Removing PII. Journey gives you an easy way to erase sensitive data once it is no longer needed. For example, your Credit Card Application graph can include a step to remove the SSN once the credit score has been computed. For an example, please see
  ```
    mutate(:ssn_redacted, [:credit_score], fn _ -> {:ok, "<redacted>"} end, mutates: :ssn)
  ```
  node in the example credit card application graph, [here](https://github.com/markmark206/journey/blob/063342e616267375a0fa042317d5984d1198cb5c/lib/journey/examples/credit_card_application.ex#L210), which mutates the contents of the :ssn node, replacing its value with "<redacted>", when :credit_score completes.

  * Tooling and visualization: `Journey.Tools` provides a set of tools for introspecting and managing executions, and for visualizing your application's graph.


  ## A (slightly) richer example: computing horoscopes

  Consider a simple Horoscope application that computes a customer's zodiac sign and horoscope based on their birthday. The application will ask the customer to `input` their name and birthday, and it then auto-`compute`s their zodiac sign and horoscope.

  This application can be thought of as a graph of nodes, where each node represents a piece of customer-provided data or the result of a computation. Add functions for computing the zodiac sign and horoscope, and capture the sequencing of the computations, and you have a graph that captures the flow of data and computations in your application. When a customer visits your application, you can start the execution of the graph, to accept and store customer-provided inputs (name, birthday), and to compute the zodiac sign and horoscope based on these inputs.

  Journey provides a way to define such graphs, and to run their executions, to serve your customer flows.

  ## Step-by-Step

  Below is a step-by-step example of defining a Journey graph for this Horoscope application.

  (These are code snippets, if you want a complete fragment you can paste into `iex` or livebook, scroll down to the "Putting together" code block.)

  This graph captures customer `input`s, and defines `compute`ations (together with their functions and prerequisites):

  ```elixir
  graph = Journey.new_graph(
    "horoscope workflow - module doctest",
    "v1.0.0",
    [
      input(:first_name),
      input(:birth_day),
      input(:birth_month),
      compute(
        :zodiac_sign,
        [:birth_month, :birth_day],
        &compute_zodiac_sign/1
      ),
      compute(
        :horoscope,
        [:first_name, :zodiac_sign],
        &compute_horoscope/1
      )
    ]
  )
  ```

  When a customer lands on your web page, and starts a new flow, your application will start a new execution of the graph,

  ```elixir
  execution = Journey.start_execution(graph)
  ```

  and it will populate the execution with the input values (name, birthday) as the customer provides them:

  ```elixir
  execution = Journey.set_value(execution, :first_name, "Mario")
  execution = Journey.set_value(execution, :birth_day, 5)
  execution = Journey.set_value(execution, :birth_month, "May")
  ```

  Providing these input values will trigger automatic computations of the customer's zodiac_sign and the horoscope, which can then be read from the execution and rendered on the web page.

  ```
  {:ok, zodiac_sign} = Journey.get_value(execution, :zodiac_sign, wait_any: true)
  {:ok, horoscope} = Journey.get_value(execution, :horoscope, wait_any: true)
  ```

  And that's it!


  ## Example

  Putting together the components of the horoscope example into a complete, running doctest example:


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

  For a more in-depth example of building a more complex application, see the Credit Card Application example in `Journey.Examples.CreditCardApplication`.

  """

  alias Journey.Executions
  alias Journey.Graph
  alias Journey.Persistence.Schema.Execution

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

  Use `start_execution/1` to create executions and `set_value/3` to populate input values.

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

  Executions can be modified by their background computations, or scheduled events, or other processes setting their values. This function is used to get the latest state of an execution -- as part of normal operations, or when the system starts up, or when the user whose session is being tracked as an execution comes back to the web site and resumes their flow.

  ## Quick Example

  ```elixir
  execution = Journey.set_value(execution, :name, "Mario")
  execution = Journey.load(execution)  # Get updated state with new revision
  {:ok, greeting} = Journey.get_value(execution, :greeting, wait_any: true)
  ```

  Use `set_value/3` and `get_value/3` to modify and read execution values.

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
    * `:filter_by` - List of node value filters using database-level filtering for optimal performance. Each filter is a tuple `{node_name, operator, value}` or `{node_name, operator}` for nil checks. Operators: `:eq`, `:neq`, `:lt`, `:lte`, `:gt`, `:gte` (comparisons), `:in`, `:not_in` (membership), `:is_nil`, `:is_not_nil` (existence). Values can be strings, numbers, booleans, nil or lists (used with `:in` and `:not_in`). Complex values (maps, tuples, functions) will raise an ArgumentError.
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

  Validation that graph_version requires graph_name:

  ```elixir
  iex> Journey.list_executions(graph_version: "v1.0.0")
  ** (ArgumentError) Option :graph_version requires :graph_name to be specified
  ```

  Sorting by execution fields and node values:

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

  Filtering with multiple operators:

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

  Creates a persistent execution in the database with a unique ID and begins background processing
  for any schedulable nodes. The execution starts with revision 0 and no values set.

  ## Quick Example

  ```elixir
  execution = Journey.start_execution(graph)
  execution = Journey.set_value(execution, :name, "Mario")
  {:ok, greeting} = Journey.get_value(execution, :greeting, wait_any: true)
  ```

  Use `set_value/3` to provide input values and `get_value/3` to retrieve computed results.

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
  * **Ready for inputs** - Can immediately accept input values via `set_value/3`

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

  This function filters the execution to only include nodes that have been populated with data.
  Unset nodes are excluded from the result. Always includes `:execution_id` and `:last_updated_at` metadata.

  ## Quick Example

  ```elixir
  execution = Journey.set_value(execution, :name, "Alice")
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

  Use `values/2` to see only current values, or `set_value/3` and `get_value/3` for individual operations.

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

  When a value is set, Journey automatically recomputes any dependent computed nodes to ensure
  consistency across the dependency graph. The operation is idempotent - setting the same value
  twice has no effect.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string
  * `node_name` - Atom representing the input node name (must exist in the graph)
  * `value` - The value to set. Supported types: nil, string, number, map, list, boolean. Note that if the map or the list contains atoms, those atoms will be converted to strings.

  ## Returns
  * Updated `%Journey.Persistence.Schema.Execution{}` struct with incremented revision (if value changed)

  ## Errors
  * Raises `RuntimeError` if the node name does not exist in the execution's graph
  * Raises `RuntimeError` if attempting to set a compute node (only input nodes can be set)

  ## Key Behaviors
  * **Automatic recomputation** - Setting a value triggers recomputation of all dependent nodes
  * **Idempotent** - Setting the same value twice has no effect (no revision increment)
  * **Input nodes only** - Only input nodes can be set; compute nodes are read-only

  ## Quick Example

  ```elixir
  execution = Journey.set_value(execution, :name, "Mario")
  {:ok, greeting} = Journey.get_value(execution, :greeting, wait_any: true)
  ```

  Use `get_value/3` to retrieve the set value and `unset_value/2` to remove values.

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
  iex> updated_execution = Journey.set_value(execution.id, :name, "Luigi")
  iex> Journey.get_value(updated_execution, :name)
  {:ok, "Luigi"}
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

  When a value is unset, Journey automatically invalidates (unsets) all computed nodes that depend
  on the unset input, creating a cascading effect through the dependency graph. This ensures data
  consistency - no computed values remain that were based on the now-removed input.

  ## Quick Example

  ```elixir
  execution = Journey.unset_value(execution, :name)
  {:error, :not_set} = Journey.get_value(execution, :name)
  ```

  Use `set_value/3` to set values and `get_value/3` to check if values are set.

  ## Parameters
  * `execution` - A `%Journey.Persistence.Schema.Execution{}` struct or execution ID string
  * `node_name` - Atom representing the input node name (must exist in the graph)

  ## Returns
  * Updated `%Journey.Persistence.Schema.Execution{}` struct with incremented revision (if value was set)

  ## Errors
  * Raises `RuntimeError` if the node name does not exist in the execution's graph
  * Raises `RuntimeError` if attempting to unset a compute node (only input nodes can be unset)

  ## Key Behaviors
  * **Cascading invalidation** - Dependent computed nodes are automatically unset
  * **Idempotent** - Multiple unsets of the same value have no additional effect
  * **Input nodes only** - Only input nodes can be unset; compute nodes cannot be unset

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
  iex> execution_after_unset = Journey.unset_value(execution, :name)
  iex> execution_after_unset.revision == original_revision
  true
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
  * `{:ok, value}` – the value is set
  * `{:error, :not_set}` – the value is not yet set
  * `{:error, :no_such_value}` – the node does not exist

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
