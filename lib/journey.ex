defmodule Journey do
  @moduledoc """

  ## TL;DR

  Journey is a library for building and executing computation graphs.

  It lets you define your application as a self-computing graph and run it without having to worry about the nitty-gritty of persistence, dependencies, scalability, or reliability.

  Consider a simple Horoscope application that computes a user's zodiac sign and horoscope based on their birthday. The application will ask the user to `input` their name and birthday, and it then auto-`compute`s their zodiac sign and horoscope.

  This application can be thought of as a graph of nodes, where each node represents a piece of user-provided data or the result of a computation. Add functions for computing the zodiac sign and horoscope, and capture the sequencing of the computations, and you have a graph that captures the flow of data and computations in your application. When a user visits your application, you can start the execution of the graph, to accept and store user provided inputs (name, birthday), and to compute the zodiac sign and horoscope based on these inputs.

  Journey provides a way to define such graphs, and to run their executions, to serve your user flows.

  ## Step-by-Step

  Below is an example of defining a Journey graph for this Horoscope application.

  This graph captures user `input`s, and defines `compute`ations (together with their functions and prerequisites):

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

  Once a customer lands on your web page, and starts a new flow, your application will start a new execution of the graph,

  ```elixir
  execution = Journey.start_execution(graph)
  ```

  and it will populate the execution the input values provided by the user (name, birthday) as they become available:

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

  ## What Exactly Does Journey Provide?

  Despite this simplicity of use, here are a few things provided by Journey that are worth noting:

  * Persistence: Executions are persisted, so if the user leaves the web site, or if the system crashes, their execution can be reloaded and continued from where it left off.

  * Scaling: Since Journey runs as part of your application, it scales with your your application, Your graph's computations (`&compute_zodiac_sign/1` and `&compute_horoscope/1` in the example above) run on the same nodes where the replicas of your application are running. No additional infrastructure or cloud services are needed.

  * Reliability: Journey uses database-based supervision of computation tasks: The `compute` functions are subject to customizable retry policy, so if `&compute_horoscope/1` fails because of a temporary glitch (e.g. the LLM service it uses for drafting horoscopes is currently overloaded), it will be retried.

  * Code Structure: The flow of your application is capture in the Journey graph, and the business logic is captured in the compute functions (`&compute_zodiac_sign/1` and `&compute_horoscope/1`). This clean separation supports you in structuring the functionality of your application in a clear, easy to understand and maintain way.

  * Conditional flow: Journey allows you to define conditions for when a node is to be unblocked. So you if your graph includes a "credit_approval_decision" node, the decision can inform which part of the graph is to be executed next (sending a "congrats!" email and starting the credit card issuance process, or sending a "sad trombone" email).

  * Graph Visualization: Journey provides tools for visualizing your application's graph, so you can easily see the flow of data and computations in your application, and to share and discuss it with your team.

  * Scheduling: Your graph can include computations that are scheduled to run at a later time, or on a recurring basis. Daily horoscope emails! A reminder email if they haven't visited the web site in a while! A "happy birthday" email!

  * Removing PII. Journey gives you an easy way to erase sensitive data once it is no longer needed. For example, your Credit Card Application graph can include a step to remove the SSN once the credit score has been computed. TODO: include links to the relevant portion of the example.

  * Tooling and visualization: Journey provides a set of tools for introspecting and managing executions, and for visualizing your application's graph.

  See the Credit Card Application example in `Journey.Examples.CreditCardApplication` for a more in-depth example of using Journey to build a more complex application.


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

  """

  alias Journey.Executions
  alias Journey.Graph
  alias Journey.Persistence.Schema.Execution

  @doc """
  Creates a new graph with the given name, version, and nodes.

  ## Examples:

    ```elixir
    iex> import Journey.Node
    iex> import Journey.Node.Conditions
    iex> import Journey.Node.UpstreamDependencies
    iex> _graph = Journey.new_graph(
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
    ```

  """
  def new_graph(name, version, nodes)
      when is_binary(name) and is_binary(version) and is_list(nodes) do
    Graph.new(name, version, nodes)
    |> Journey.Graph.Validations.validate()
    |> Graph.Catalog.register()
  end

  @doc """
  Loads the latest version of the execution with the supplied ID.

  Archived executions are not loaded by default, but can be included by setting the `include_archived: true` option.

  ## Parameters:
  - `execution_id` – the ID of the execution to load, or a `%Journey.Persistence.Schema.Execution{}` struct.

  ## Options:
  - `opts` – a keyword list of options. Supported options:
    - `:preload` – whether to preload the execution's nodes and values. Defaults to `true`.
    - `:include_archived` – whether to include archived executions. Defaults to `false`. If set to `true`, archived executions will be loaded even if they are not visible in the list of executions.

  ## Returns:
  - A `%Journey.Persistence.Schema.Execution{}` struct representing the loaded execution, or `nil` if the execution does not exist or is archived and not included.
  - If the execution is loaded successfully, it will have a `revision` field indicating the version of the execution.
  - If the execution is not found, it will return `nil`.
  - If the execution is archived and `include_archived: false` (or if `include_archived: ` is missing), it will return `nil`.

  ## Examples:

  ```elixir
  iex> graph = Journey.Examples.Horoscope.graph()
  iex> execution = Journey.start_execution(graph)
  iex> execution.revision
  0
  iex> # This is only needed in a test, to perform background processing that happens automatically outside of tests.
  iex> background_sweeps_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)
  iex> execution |> Journey.set_value(:birth_day, 26) |> Journey.set_value(:birth_month, 4) |> Journey.set_value(:first_name, "Mario")
  iex> # Wait for the computations to complete, and reload the execution, which will now have a new revision.
  iex> Journey.get_value(execution, :library_of_congress_record, wait_any: true)
  {:ok, "Mario's horoscope was submitted for archival."}
  iex> execution = execution.id |> Journey.load()
  iex> execution.revision
  9
  iex> # This is only needed in tests.
  iex> Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_sweeps_task)
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
  Lists existing executions.

  Archived executions are not included by default, but can be included by setting the `include_archived: true` option.

  ## Options:
  - `:graph_name` – the name of the graph for which to list executions.
  - `:order_by_execution_fields` – a list of fields by which to order the executions. Defaults to `[:updated_at]`.
  - `:value_filters` – a list of filters to apply to the execution values. Each filter is a tuple in the format `{node_name, operator, value}` or `{node_name, function, value}`. Supported operators are `:eq`, `:gt`, `:gte`, `:lt`, `:lte`. The function can be any function that takes two arguments and returns a boolean.
  - `:limit` – the maximum number of executions to return. Defaults to `10_000`.
  - `:offset` – the offset from which to start returning executions. Defaults to `0`.
  - `:include_archived` – whether to include archived executions. Defaults to `false`.

  ## Returns:
  - A list of `%Journey.Persistence.Schema.Execution{}` structs representing the executions that match the given criteria.

  ## Examples:

  ```elixir
  iex> graph = Journey.Examples.Horoscope.graph()
  iex> for day <- 1..20, do: Journey.start_execution(graph) |> Journey.set_value(:birth_day, day) |> Journey.set_value(:birth_month, 4) |> Journey.set_value(:first_name, "Mario")
  iex> executions = Journey.list_executions(graph_name: graph.name, order_by_execution_fields: [:inserted_at])
  iex> Enum.count(executions)
  20
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, :eq, 1}]) |> Enum.count()
  1
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, :gt, 7}]) |> Enum.count()
  13
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, :gte, 7}]) |> Enum.count()
  14
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, :lt, 7}]) |> Enum.count()
  6
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, :lte, 7}]) |> Enum.count()
  7
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, :eq, 10}]) |> Enum.count()
  1
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, fn a, b -> a == b end, 10}]) |> Enum.count()
  1
  iex> Journey.list_executions(graph_name: graph.name, value_filters: [{:birth_day, fn a -> a in [9, 12] end}]) |> Enum.count()
  2
  iex> Journey.list_executions(graph_name: graph.name, limit: 3) |> Enum.count()
  3
  iex> Journey.list_executions(graph_name: graph.name, limit: 10, offset: 15) |> Enum.count()
  5
  iex> Journey.list_executions(graph_name: graph.name, include_archived: true) |> Enum.count()
  20
  ```

  """

  def list_executions(options \\ []) do
    check_options(options, [:graph_name, :order_by_execution_fields, :value_filters, :limit, :offset, :include_archived])

    value_filters = Keyword.get(options, :value_filters, [])
    limit = Keyword.get(options, :limit, 10_000)
    offset = Keyword.get(options, :offset, 0)

    graph_name = Keyword.get(options, :graph_name, nil)
    order_by_field = Keyword.get(options, :order_by_execution_fields, [:updated_at])
    include_archived = Keyword.get(options, :include_archived, false)

    Journey.Executions.list(graph_name, order_by_field, value_filters, limit, offset, include_archived)
  end

  @doc """
  Starts a new execution of the given graph.

  ## Parameters:
  - `graph`: The graph for which to start an execution. Must be a `%Journey.Graph{}` struct.

  ## Returns:
  - A new `%Journey.Persistence.Schema.Execution{}` struct representing the started execution.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "horoscope workflow - start_execution doctest",
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
  "horoscope workflow - start_execution doctest"
  iex> execution.graph_version
  "v1.0.0"
  iex> execution.revision
  0
  ```

  """
  def start_execution(graph) when is_struct(graph, Graph) do
    Journey.Executions.create_new(
      graph.name,
      graph.version,
      graph.nodes
    )
    |> Journey.Scheduler.advance()
  end

  @doc """
  Returns a map containing all nodes in the execution with their current status.

  Unlike `values/2` which only returns nodes with set values, this function returns all nodes
  including those that haven't been set yet (marked as `:not_set`). Set values are returned
  as tuples in the format `{:set, value}`.

  ## Options:
  * `:reload` – whether to reload the execution before fetching the values. Defaults to `true`.

  ## Examples:

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "horoscope workflow - schedule_recurring doctest",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         input(:last_name),
  ...>         input(:district)
  ...>        ]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> Journey.values_all(execution) |> redact([:execution_id, :last_updated_at])
  %{name: :not_set, district: :not_set, last_name: :not_set, execution_id: {:set, "..."}, last_updated_at: {:set, 1234567890}}
  iex> execution = execution |> Journey.set_value(:name, "Mario")
  iex> Journey.values_all(execution) |> redact([:execution_id, :last_updated_at])
  %{district: :not_set, last_name: :not_set, name: {:set, "Mario"}, execution_id: {:set, "..."}, last_updated_at: {:set, 1234567890}}
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
  Returns a map of all nodes in the execution that have been set, with their values.

  ## Options:
  * `:reload` – whether to reload the execution before fetching the values. Defaults to `true`.

  ## Examples

  ```elixir
  iex> import Journey.Node
  iex> execution =
  ...>    Journey.Examples.Horoscope.graph() |>
  ...>    Journey.start_execution() |>
  ...>    Journey.set_value(:birth_day, 26)
  iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
  %{birth_day: 26, execution_id: "...", last_updated_at: 1234567890}
  iex> execution = Journey.set_value(execution, :birth_month, "April")
  iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
  %{birth_day: 26, birth_month: "April", execution_id: "...", last_updated_at: 1234567890}
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

  This function provides visibility into the order of operations that occurred during
  an execution, including both computations that completed successfully and values that were set.

  ## Parameters
  - `execution` or `execution_id`: The execution struct or ID to get history for

  ## Returns
  A list of maps sorted by execution revision, where each map contains:
  - `:computation_or_value` - either `:computation` or `:value`
  - `:node_name` - the name of the node
  - `:node_type` - the type of the node
  - `:revision` - the execution revision when this operation completed
  - `:value` - the actual value (only present for value nodes)

  ## Examples

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>   "workflow with history",
  ...>   "v1.0.0",
  ...>   [
  ...>     input(:x),
  ...>     input(:y),
  ...>     compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end)
  ...>   ]
  ...> )
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
  * `value` - The value to set. Supported types: nil, string, number, map, list, boolean, atom

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
                is_boolean(value) or is_atom(value)) do
    # Load execution without preloading associations
    execution = Journey.Repo.get!(Execution, execution_id)

    # Validate using the existing execution-based validation (it will fetch the graph from catalog)
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)

    # Use the execution_id version
    Journey.Executions.set_value(execution_id, node_name, value)
  end

  def set_value(execution, node_name, value)
      when is_struct(execution, Execution) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value) or is_atom(value)) do
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
  ...>           fn %{name: name} ->
  ...>             {:ok, "Hello, \#{name}!"}
  ...>           end
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
  iex> graph = Journey.new_graph("unset workflow - idempotent example", "v1.0.0", [input(:name)])
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
    Journey.Graph.Validations.ensure_known_input_node_name(execution, node_name)
    Journey.Executions.unset_value(execution, node_name)
  end

  def unset_value(execution, node_name)
      when is_struct(execution, Execution) and is_atom(node_name) do
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

    Journey.Graph.Validations.ensure_known_node_name(execution, node_name)

    wait_new = Keyword.get(opts, :wait_new, false)
    wait_any = Keyword.get(opts, :wait_any, false)

    # Check for mutually exclusive options
    if wait_new != false and wait_any != false do
      raise ArgumentError, "Options :wait_any and :wait_new are mutually exclusive"
    end

    timeout_ms_or_infinity = determine_timeout(wait_new, wait_any)

    Executions.get_value(execution, node_name, timeout_ms_or_infinity, wait_new: wait_new != false)
  end

  @doc """
  Archives the supplied execution.

  TODO: "A node defined as an "archive()" node..."

  TODO: include an example of defining an archive node in a graph -- inline, or by linking it to the credit card application example.

  Once an execution is archived, it is no longer visible in the list of executions, and cannot be loaded unless explicitly requested with the `include_archived: true` option. The background processing of the execution will stop.

  ## Parameters:
  - `execution` or `execution_id`: The execution to archive, or the ID of the execution to archive.

  Returns
  * the time (unix epoch in seconds) of the execution's archived_at timestamp.

  ## Examples

    ```elixir
    iex> execution =
    ...>    Journey.Examples.Horoscope.graph() |>
    ...>    Journey.start_execution() |>
    ...>    Journey.set_value(:birth_day, 26)
    iex> execution.archived_at
    nil
    iex> archived_at = Journey.archive(execution)
    iex> archived_at == nil
    false
    iex> # Archived executions are invisible.
    iex> Journey.load(execution)
    nil
    iex> # Archiving an archived execution has no effect.
    iex> archived_at == Journey.archive(execution)
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
