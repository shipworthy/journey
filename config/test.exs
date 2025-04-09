import Config

config :journey, Journey.Repo,
  database: "journey_test",
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  queue_target: 4_000,
  queue_interval: 8_000,
  timeout: 60_000,
  pool_size: 20,
  pool: Ecto.Adapters.SQL.Sandbox,
  ownership_timeout: 700_000,
  port: 5438

config :journey, ecto_repos: [Journey.Repo]

config :logger,
       :console,
       format: "$time [$level] $metadata$message\n",
       level: :debug,
       metadata: [:pid]
