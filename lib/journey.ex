defmodule Journey do
  @moduledoc """

  Journey lets you define, maintain, and execute computation graphs, with persistence, clarity, and scalability.

  For example, a web application that computes horoscopes can be modeled with a graph containing nodes for the user's **name** and **birthday** (supplied by the user), and for the user's **zodiac sign** and **horoscope** (auto-computed based on the user's name and birthday).

  Once a user starts the flow on your website, your application starts a new execution of the graph, populates it with the data provided by the user (**name**, **birthday**), and reads the data that gets auto-computed by the graph (**zodiac sign**, **horoscope**).

  Every node's data is persisted, so if the user leaves the website, or the system crashes, the execution can be reloaded and continued from where it left off.

  Since computations run as part of your application, on any node where your application is running, your system is as scalable and distributed as you need it to be, without requiring any additional infrastructure or cloud services.

  The essence of your functionality is captured in the graph, so your code is clear and easy to maintain.

  Here's an example of defining such a graph and executing an instance of it:

  ## Examples

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
  iex> Journey.get_value(e, :zodiac_sign, wait: true)
  {:ok, "Taurus"}
  iex> Journey.values(e)
  %{birth_day: 26, birth_month: "April", zodiac_sign: "Taurus"}
  iex>
  iex> # 5. Once we get :first_name, the :horoscope node will compute itself:
  iex> e = Journey.set_value(e, :first_name, "Mario")
  iex> Journey.get_value(e, :horoscope, wait: true)
  {:ok, "🍪s await, Taurus Mario!"}
  iex>
  iex> Journey.values(e)
  %{birth_day: 26, birth_month: "April", first_name: "Mario", horoscope: "🍪s await, Taurus Mario!", zodiac_sign: "Taurus"}
  iex>
  iex> # 6. and we can always list executions.
  iex> this_execution = Journey.list_executions(graph_name: "horoscope workflow - module doctest", order_by_execution_fields: [:inserted_at]) |> Enum.reverse() |> hd
  iex> e.id == this_execution.id
  true
  ```

  """

  alias Journey.Execution
  alias Journey.Executions
  alias Journey.Graph

  @doc """
  Creates a new graph with the given name, version, and nodes.

  ## Examples:

    ```elixir
    iex> import Journey.Node
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

  ## Examples:

  ```elixir
  iex> graph = Journey.Examples.Horoscope.graph()
  iex> execution = Journey.start_execution(graph)
  iex> execution.revision
  0
  iex> execution |> Journey.set_value(:birth_day, 26) |> Journey.set_value(:birth_month, 4) |> Journey.set_value(:first_name, "Mario")
  iex> # Wait for the computations to complete, and reload the execution, which will now have a new revision.
  iex> Journey.get_value(execution, :library_of_congress_record, wait: true)
  {:ok, "Mario's horoscope was submitted for archival."}
  iex> execution = execution.id |> Journey.load()
  iex> execution.revision
  9
  ```

  """
  def load(nil), do: nil

  def load(execution_id) when is_binary(execution_id) do
    Journey.Executions.load(execution_id)
  end

  def load(execution) when is_struct(execution, Execution) do
    load(execution.id)
  end

  @doc """
  Lists existing executions.

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
  ```

  """

  def list_executions(options \\ []) do
    known_option_names = MapSet.new([:graph_name, :order_by_execution_fields, :value_filters, :limit, :offset])
    supplied_option_names = MapSet.new(Keyword.keys(options))
    unexpected_option_names = MapSet.difference(supplied_option_names, known_option_names)
    value_filters = Keyword.get(options, :value_filters, [])
    limit = Keyword.get(options, :limit, 10_000)
    offset = Keyword.get(options, :offset, 0)

    if unexpected_option_names != MapSet.new([]) do
      raise ArgumentError,
            "Unknown options: #{inspect(MapSet.to_list(unexpected_option_names))}. Known options: #{inspect(MapSet.to_list(known_option_names) |> Enum.sort())}."
    end

    graph_name = Keyword.get(options, :graph_name, nil)
    order_by_field = Keyword.get(options, :order_by_execution_fields, [:updated_at])

    Journey.Executions.list(graph_name, order_by_field, value_filters, limit, offset)
  end

  @doc """
  Starts a new execution of the given graph.

  ## Parameters:
  - `graph`: The graph for which to start an execution. Must be a `%Journey.Graph{}` struct.

  ## Returns:
  - A new `%Journey.Execution{}` struct representing the started execution.

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
  iex> Journey.values_all(execution)
  %{name: :not_set, district: :not_set, last_name: :not_set}
  iex> execution = execution |> Journey.set_value(:name, "Mario")
  iex> Journey.values_all(execution)
  %{district: :not_set, last_name: :not_set, name: {:set, "Mario"}}
  ```

  """
  def values_all(execution) when is_struct(execution, Execution) do
    Executions.values(execution)
  end

  @doc """
  Returns a map of all nodes in the execution that have been set, with their values.

  ## Options:
  * `:reload` – whether to reload the execution before fetching the values. Defaults to `true`.

  ## Examples

  ```elixir
  iex> execution =
  ...>    Journey.Examples.Horoscope.graph() |>
  ...>    Journey.start_execution() |>
  ...>    Journey.set_value(:birth_day, 26)
  iex> Journey.values(execution)
  %{birth_day: 26}
  iex> execution = Journey.set_value(execution, :birth_month, "April")
  iex> Journey.values(execution)
  %{birth_day: 26, birth_month: "April"}
  ```

  """
  def values(execution, opts \\ []) when is_struct(execution, Execution) and is_list(opts) do
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
  Sets the value for an input node in an execution.

  ## Examples

  ```elixir
  iex> import Journey.Node
  iex> graph = Journey.new_graph(
  ...>       "horoscope workflow - set_value doctest",
  ...>       "v1.0.0",
  ...>       [
  ...>         input(:name),
  ...>         input(:last_name),
  ...>         input(:district)
  ...>       ]
  ...>     )
  iex> execution = graph |> Journey.start_execution()
  iex> execution = Journey.set_value(execution, :name, "Mario")
  iex> execution |> Journey.values()
  %{name: "Mario"}
  iex> execution |> Journey.values_all()
  %{name: {:set, "Mario"}, district: :not_set, last_name: :not_set}
  ```

  """
  def set_value(execution, node_name, value)
      when is_struct(execution, Execution) and is_atom(node_name) and
             (value == nil or is_binary(value) or is_number(value) or is_map(value) or is_list(value) or
                is_boolean(value) or is_atom(value)) do
    ensure_known_input_node_name(execution, node_name)
    Journey.Executions.set_value(execution, node_name, value)
  end

  @doc """
  Returns the value of a node in an execution. Optionally waits for the value to be set.

  ## Options:
  * `:wait` – whether or not to wait for the value to be set. This option can have the following values:
    * `false` or `0` – return immediately without waiting (default)
    * `true` – wait until the value is available, or until timeout
    * a positive integer – wait for the supplied number of milliseconds (default: 5_000)
    * `:infinity` – wait indefinitely

  Returns
  * `{:ok, value}` – the value is set
  * `{:error, :not_set}` – the value is not yet set
  * `{:error, :no_such_value}` – the node does not exist

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
    iex> Journey.get_value(execution, :astrological_sign, wait: true)
    {:ok, "Taurus"}
    iex> Journey.get_value(execution, :horoscope, wait: 2_000)
    {:error, :not_set}
    iex> execution = Journey.set_value(execution, :first_name, "Mario")
    iex> Journey.get_value(execution, :horoscope, wait: true)
    {:ok, "🍪s await, Taurus Mario!"}
    ```

  """
  def get_value(execution, node_name, opts \\ [])
      when is_struct(execution, Execution) and is_atom(node_name) and is_list(opts) do
    ensure_known_node_name(execution, node_name)
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

  # TODO: move to a dedicated module, validations.ex or some such. invoke from executions.ex.
  defp ensure_known_node_name(execution, node_name) do
    all_node_names = execution.values |> Enum.map(& &1.node_name)

    if node_name in all_node_names do
      :ok
    else
      raise "'#{inspect(node_name)}' is not a known node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid node names: #{inspect(Enum.sort(all_node_names))}."
    end
  end

  # TODO: move to a dedicated module, validations.ex or some such. invoke from executions.ex.
  defp ensure_known_input_node_name(execution, node_name)
       when is_struct(execution, Journey.Execution) and is_atom(node_name) do
    graph = Journey.Graph.Catalog.fetch!(execution.graph_name)

    all_input_node_names =
      graph.nodes
      |> Enum.filter(fn n -> n.type == :input end)
      |> Enum.map(& &1.name)

    if node_name in all_input_node_names do
      :ok
    else
      raise "'#{inspect(node_name)}' is not a valid input node in execution '#{execution.id}' / graph '#{execution.graph_name}'. Valid input node names: #{inspect(Enum.sort(all_input_node_names))}."
    end
  end
end
