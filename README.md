# Overview


<div align="right">
"Start simple, go far."<br>
— <a href="https://www.youtube.com/watch?v=JvBT4XBdoUE">Saša Jurić, "The Soul of Elixir and Erlang"</a>
</div>


## What is Journey

A persisted, reactive graph package, Journey eliminates the boilerplate code you normally have to write (over and over again) when implementing multi-step processes with persistence - state tracking and orchestration code, retry logic, persistence itself.

For durable processes of any size (a linear, 2-3 step process or a 60 step conditional flow), Journey helps you define your durable process as a simple dependency graph where inputs automatically trigger the right computations in the right order, with retries, on any of your application's replicas, and with everything persisted to PostgreSQL.

You can use Journey for 3-steps flows (such as this simple "welcome" linear flow

```elixir
import Journey.Node
graph = Journey.new_graph(
   "demo welcome graph – readme doctest",
   "v1",
   [
     input(:name),
     input(:email_address),
     compute(:greeting, [:name, :email_address], &send_welcome_email/1)
   ]
)
```
), or for multi-step conditional flows.

Regardless of the size and nature of the workflow, for BOTH tiny-and-linear AND large-and-conditional durable flows, Journey saves you the work of writing and maintaining tedious boilerplate code (persistence, orchestration, retries, scalability), and lets you focus on the actual functionality of your application.

You can build your workflow with GenServers, ETS/Mnesia/postgres, shared state, db schemas, custom retry logic, state reloading logic, and the code that would safely run send_welcome_email at the right time, even as your system gets rebooted or scales up and down or survives an outage. Journey eliminates the need for writing that boilerplate code, so you can focus on your business logic – regardless of whether you are building tiny-and-linear or large-and-conditional durable flows.

So, is Journey just for complex (large, conditional) flows? Not at all! Journey lets your application provide durable, reliable, scalable execution, regardless of the complexity of your business logic. In fact, somewhat ironically, the amount of boilerplate code that you would otherwise need to write – and that Journey handles for you – for a simple, two-step linear process to make it durable and reliable, is disproportionately large, as percentage of your application's code.

Executions of Journey graphs survive crashes, redeploys, page reloads, while scaling naturally with your application - no additional infrastructure or cloud service$ required.

Your application can perform durable, short or long-running executions, with retries, scalability, dependency tracking, scheduling and analytics.

Journey's primitives are simple: graph, dependencies, functions, persistence, retries, scheduling. Together, they help you build rich, scalable, reliable functionality with simple, well-structured and easy-to-understand code, quickly.

A tiny two-step sequence (see the tiny [Useless Machine](https://github.com/markmark206/journey/blob/main/lib/examples/useless_machine.ex) example), or a large multi-step application with conditional processing – Journey provides simple persistence, scalability and resilience.


## Basic Concepts

To illustrate a few concepts (graph, dependencies – including conditional dependencies, computation functions, persistence), here is a slightly more complex example.

Keep in mind that these concepts apply for both simple, two-step linear processes and complex, multi-step conditional processes. Journey makes it easy to implement durable flows of a wide ranges of size and complexity.

This graph adds two numbers when they become available, and conditionally sets the "too large" flag.


```elixir
iex> import Journey.Node
iex> # Defining a graph, with two input nodes and two downstream computations.
iex> graph = Journey.new_graph(
...>   "demo graph – doctest",
...>   "v1",
...>   [
...>     input(:x),
...>     input(:y),
...>     # :sum is unblocked when :x and :y are provided.
...>     compute(:sum, [:x, :y], fn %{x: x, y: y} -> {:ok, x + y} end),
...>     # :large_value_alert is unblocked when :sum is provided and is greater than 40.
...>     compute(
...>         :large_value_alert,
...>         [sum: fn sum_node -> sum_node.node_value > 40 end],
...>         fn %{sum: sum} -> {:ok, "🚨, at #{sum}"} end,
...>         f_on_save: fn _execution_id, _result ->
...>            # (e.g. send a pubsub notification to the LiveView process to update the UI)
...>            :ok
...>         end
...>     )
...>   ]
...> )
iex> # Start an execution of this graph, set input values, read computed values.
iex> execution = Journey.start_execution(graph)
iex> execution = Journey.set(execution, :x, 12)
iex> execution = Journey.set(execution, :y, 2)
iex> {:ok, %{value: 14}} = Journey.get_value(execution, :sum, wait: :any)
iex> Journey.get_value(execution, :large_value_alert)
{:error, :not_set}
iex> eid = execution.id
iex> # After an outage / redeployment / page reload / long pause, an execution
iex> # can be reloaded and continue, as if nothing happened.
iex> execution = Journey.load(eid)
iex> # An update to :y triggers a re-computation of downstream values.
iex> execution = Journey.set(execution, :y, 37)
iex> {:ok, %{value: "🚨, at 49"}} = Journey.get_value(execution, :large_value_alert, wait: :any)
iex> Journey.values(execution) |> redact([:execution_id, :last_updated_at])
%{execution_id: "...", last_updated_at: 1234567890, sum: 49, x: 12, y: 37, large_value_alert: "🚨, at 49"}
```

The graph can be visualized as a Mermaid graph:

```
> Journey.Tools.generate_mermaid_graph(graph)
graph TD
  %% Graph
  subgraph Graph["🧩 'demo graph', version v1"]
      execution_id[execution_id]
      last_updated_at[last_updated_at]
      x[x]
      y[y]
      sum["sum<br/>(anonymous fn)"]
      large_value_alert["large_value_alert<br/>(anonymous fn)"]

      x -->  sum
      y -->  sum
      sum -->  large_value_alert
  end

  %% Styling
  classDef inputNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000000
  classDef computeNode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000000
  classDef scheduleNode fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000000
  classDef mutateNode fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px,color:#000000

  %% Apply styles to actual nodes
  class y,x,last_updated_at,execution_id inputNode
  class large_value_alert,sum computeNode
```

A few things to note about this example:
* Every input value (`:x`, `:y`), or computation result (`:sum`, `:large_value_alert`) is persisted as soon as it becomes available,
* The functions attached to `:sum` and `:large_value_alert`
  - are called reliably, with a retry policy,
  - will execute on any of the replicas of your application,
  - are called proactively – when their upstream dependencies are available.
* Executions of this flow can take as long as needed (milliseconds? months?), and will live through system restarts, crashes, redeployments, page reloads, etc.


## What Does Journey Provide?

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
  "horoscope workflow - module doctest (abbreviated)",
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
execution = Journey.set(execution, :first_name, "Mario")
execution = Journey.set(execution, :birth_day, 5)
execution = Journey.set(execution, :birth_month, "May")
```

Providing these input values will trigger automatic computations of the customer's zodiac_sign and the horoscope, which can then be read from the execution and rendered on the web page.

```elixir
{:ok, %{value: zodiac_sign}} = Journey.get_value(execution, :zodiac_sign, wait: :any)
{:ok, %{value: horoscope}} = Journey.get_value(execution, :horoscope, wait: :any)
```

And that's it!


## Putting It All Together

Putting together the components of the horoscope example into a complete, running doctest example:


```elixir
iex> # 1. Define a graph capturing the data and the logic of the application -
iex> #    the nodes, their dependencies, and their computations:
iex> import Journey.Node
iex> graph = Journey.new_graph(
...>       "horoscope workflow - module doctest (all together now)",
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
...>             {:ok, "🍪s await, #{zodiac_sign} #{name}!"}
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
iex> {:ok, %{value: "Taurus"}} = Journey.get_value(e, :zodiac_sign, wait: :any)
iex> Journey.values(e) |> redact([:execution_id, :last_updated_at])
%{birth_day: 26, birth_month: "April", zodiac_sign: "Taurus", execution_id: "...", last_updated_at: 1234567890}
iex>
iex> # 5. Once we get :first_name, the :horoscope node will compute itself:
iex> e = Journey.set(e, :first_name, "Mario")
iex> {:ok, %{value: "🍪s await, Taurus Mario!"}} = Journey.get_value(e, :horoscope, wait: :any)
iex>
iex> Journey.values(e) |> redact([:execution_id, :last_updated_at])
%{birth_day: 26, birth_month: "April", first_name: "Mario", horoscope: "🍪s await, Taurus Mario!", zodiac_sign: "Taurus", execution_id: "...", last_updated_at: 1234567890}
iex>
iex> # 6. and we can always list executions.
iex> this_execution = Journey.list_executions(graph_name: "horoscope workflow - module doctest (all together now)", order_by_execution_fields: [:inserted_at]) |> Enum.reverse() |> hd
iex> e.id == this_execution.id
true
```

For a more in-depth example of building a more complex application, see the Credit Card Application example in `Journey.Examples.CreditCardApplication`.

## Installation and Configuration

To use Journey in your application, you will need to install the package, configure its db, optionally configure its logging, and tell it about the graphs you want Journey to be aware of.

1. The package can be installed by adding `journey` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:journey, "~> 0.10"}
  ]
end
```

2. Journey uses Postgres DB for persistence. Add Journey Postgres DB to your project's configuration.

Alongside your app's Repo configuration, add Journey's. For example, if you want to use Journey in your Phoenix application, you might do something like:

`config/test.exs`:
```elixir
config :journey, Journey.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "demo_journey_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool_size: System.schedulers_online() * 2
```

`config/dev.exs`:
```elixir
config :journey, Journey.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "demo_journey_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10,
  log: false
```

`config/runtime.exs`:
```elixir
if config_env() == :prod do
  ...
  database_journey_url =
    System.get_env("DATABASE_JOURNEY_URL") ||
      raise """
      environment variable DATABASE_JOURNEY_URL is missing.
      For example: ecto://USER:PASS@HOST/DATABASE
      """

  config :journey, Journey.Repo,
    # ssl: true,
    url: database_journey_url,
    pool_size: String.to_integer(System.get_env("POOL_SIZE_JOURNEY") || "10"),
    socket_options: maybe_ipv6
  ...
end
```

3. Configure the level of logging you want to see from Journey

Example:
`config/config.exs`:
```elixir
config :journey, log_level: :warning
```

4. Tell Journey which graphs it should know about:

`config/config.exs`:
```elixir
config :journey, :graphs, [
  # This is just an example graph that ships with Journey.
  &Journey.Examples.CreditCardApplication.graph/0,

  # When you define functions that create graphs in your application, add them here.
  ...
]
```

## Questions / Comments / Issues

To get in touch, report an issue, or ask a question, please create a github issue: https://github.com/markmark206/journey/issues


## Full Documentation

Documentation can be found at <https://hexdocs.pm/journey>.


## An Example Phoenix Application

For an example Horoscope Phoenix application, see https://github.com/shipworthy/journey-demo

The application uses Journey to track the state of each session.

The application is hosted at https://demo.gojourney.dev/
