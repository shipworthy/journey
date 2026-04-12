# Overview


<div align="right">
"Start simple, go far."<br>
— <a href="https://www.youtube.com/watch?v=JvBT4XBdoUE">Saša Jurić, "The Soul of Erlang and Elixir"</a>
</div>

## Journey: Durable Workflows, as a Package

Journey is a package for defining and running durable workflows, with persistence, horizontal scalability, orchestration, retries, crash recovery, scheduling, introspection and analytics. 

## Example

*(some output omitted for brevity)*

### Defining a Workflow as a Graph

```elixir
iex> import Journey.Node

iex> graph = Journey.new_graph(
  "Onboarding",
  "v1",
  [
    input(:name),
    input(:email_address),
    compute(
      :greeting, 
      [:name, :email_address],
      fn values -> 
        welcome = "Welcome, #{values.name} at #{values.email_address}"
        IO.puts(welcome)
        {:ok, welcome}
      end)
  ]
)
```

### Running an execution of the graph:

```elixir
iex> execution = 
  graph
  |> Journey.start_execution()
  |> Journey.set(:name, "Mario")
  |> Journey.set(:email_address, "mario@example.com")

"Welcome, Mario at mario@example.com"

iex> Journey.values(execution) |> IO.inspect()
%{
  name: "Mario",
  email_address: "mario@example.com",
  greeting: "Welcome, Mario at mario@example.com",
}
```

Each value is persisted to PostgreSQL as soon as it is set (`:name`, `:email_address`) or computed (`:greeting`).

The greeting function executes reliably — with retries if needed — on any replica, across restarts, re-deployments, and outages.

### Introspecting an in-flight execution

"Luigi has yet to receive his greeting. Why?"


```elixir
iex> execution = 
  graph
  |> Journey.start_execution() 
  |> Journey.set(:name, "Luigi")

iex> Journey.values(execution)
%{
  name: "Luigi"
}
```

"Because we don't yet have his `:email_address`! Look:"

```elixir
iex> execution.id
  |> Journey.Tools.introspect()
  |> IO.puts()

"""
Values:
- Set:
  - name: '"Luigi"' | :input
    set at 2025-11-13 07:25:15Z | rev: 1


- Not set:
  - email_address: <unk> | :input
  - greeting: <unk> | :compute

Computations:
- Completed:


- Outstanding:
  - greeting: ⬜ :not_set (not yet attempted) | :compute
       :and
        ├─ ✅ :name | &provided?/1 | rev 1
        └─ 🛑 :email_address | &provided?/1
"""
```

### "My infrastructure is back after an outage. Can I resume executions?"

Sure! As long as you have the ID of an execution, you can simply reload it and continue, as if nothing happened.

Handling interruptions – infrastructure outages, re-deployments, scaling events, users reloading pages, or leaving and coming back later – is as easy as calling `Journey.load/1`.

```elixir
iex> execution.id |> IO.inspect()
"EXEC7BM701T4EGEG996X6BRY"

iex> execution = Journey.load("EXEC7BM701T4EGEG996X6BRY")

iex> Journey.set(execution, :email_address, "luigi@example.com")
"Welcome, Luigi at luigi@example.com"

iex> Journey.values(execution)
%{
  name: "Luigi",
  email_address: "luigi@example.com",
  greeting: "Welcome, Luigi at luigi@example.com"
}
```

### Analytics?

"Is everyone getting greeted?" "Yes, 100%!"

```elixir
iex> Journey.Insights.FlowAnalytics.flow_analytics(graph.name, graph.version) |> Journey.Insights.FlowAnalytics.to_text() |> IO.puts()
Graph: 'Welcome'
Version: 'v1'

EXECUTION STATS:
----------
Total executions: 2

NODE STATS (3 nodes):
----------
Node Name: 'email_address'
Type: input
Reached by: 2 executions (100%)
Average time to reach: 760 seconds
Flow ends here: 0 executions (0.0% of all, 0.0% of reached)

Node Name: 'greeting'
Type: compute
Reached by: 2 executions (100%)
Average time to reach: 760 seconds
Flow ends here: 0 executions (0.0% of all, 0.0% of reached)

Node Name: 'name'
Type: input
Reached by: 2 executions (100%)
Average time to reach: 8 seconds
Flow ends here: 0 executions (0.0% of all, 0.0% of reached)
```

### What Did I Just See?

The examples above demonstrated Journey's basic functionality:

* **defining a workflow** with its data points, its computations, and its dependencies, with `Journey.new_graph/4`,
* **running executions** of the workflow, and watching computations run their course when unblocked (`Journey.start_execution/1`, `Journey.set/3`, `Journey.values/2`),
* **introspecting** the state of an execution with `Journey.Tools.introspect/1`,
* **reloading** an execution after an interruption with `Journey.load/2`,
* getting basic aggregated **analytics** with `Journey.Insights.FlowAnalytics.flow_analytics/3`.

Notice how the graph captures your orchestration logic in a readable, self-documenting structure.

### What Did I Not See?

Some features not shown in this example:

- executing one-time or recurring scheduled events with `Journey.Node.tick_once/4` and `Journey.Node.tick_recurring/4` nodes, 
- mutating values with `Journey.Node.mutate/4` nodes, 
- defining conditional workflows with `Journey.Node.UpstreamDependencies.unblocked_when/2`, 
- recording the history of changes with `Journey.Node.historian/3` nodes,
- archiving executions with `Journey.Node.archive/3` nodes,
- emitting change notification events with `f_on_save/3`

You can see some of this functionality in JourDash, a play-demo food delivery service, running on https://jourdash.gojourney.dev Its source code is available on GitHub: https://github.com/shipworthy/jour_dash


## Can I Just Write This by Hand?

Absolutely!

Implementing persistence, retries, orchestrations, the logic for resuming after a crash, horizontal scalability, scheduling, tracking the history of changes, tooling for introspecting the state of individual executions and of the system, figuring out ways to structure the code... many of us have implemented – and debugged – this important non-trivial plumbing multiple times!

Journey handles those things for you, saving you the complexity of writing and maintaining thousands of lines of non-trivial plumbing code.

Instead, you can give your attention to building your actual application.


## Is Journey a SaaS? Nope.

Journey is merely a package, so you get all the goodies of executing durable workflows by simply importing it in your application and pointing it to a PostgreSQL database.

No third-party services, no extra infrastructure, no remote runtime dependencies. All your data stays with you.

Journey is durable workflows in a package.

## Installation and Configuration

Journey setup has four steps:

1. The package can be installed by adding `journey` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:journey, "~> 0.10"}
  ]
end
```

2. Journey uses PostgreSQL for persistence.

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

To get in touch, report an issue, or ask a question, please create a GitHub issue: https://github.com/shipworthy/journey/issues


## Full Documentation

Documentation can be found at <https://hexdocs.pm/journey>.


## Example Applications

Two open-source demo apps:


### JourDash Food Delivery Service

JourDash is a play-demo food delivery service. It uses Journey to conduct its food "deliveries" – from pickup to drop-off (or handoff!).

You can see the application running on https://jourdash.gojourney.dev/

The source is available on GitHub: https://github.com/shipworthy/jour_dash


### Horoscopes

Horoscopes is a Phoenix application for computing "horoscopes".

It uses Journey to orchestrate the visitor experience, while giving the user a chance to peek behind the scenes.

The application is running at https://horoscopes.gojourney.dev

Its source is available on GitHub: https://github.com/shipworthy/journey_horoscopes
