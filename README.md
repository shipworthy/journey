# Journey

Journey is an Elixir library for building and executing computation graphs with built-in persistence, reliability, and scalability.

Define your application workflows as dependency graphs where user inputs automatically trigger computations in the correct order, with all state persisted to PostgreSQL.

Your flows survive crashes, redeploys, page reloads, while scaling naturally with your application - no additional infrastructure or cloud service$ required.

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
