import Config

config :journey, Journey.Repo,
  database: "journey_dev",
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  port: 5432,
  queue_target: 1000,
  queue_interval: 2000

config :journey, ecto_repos: [Journey.Repo]

config :logger,
       :console,
       format: "$time [$level] $metadata$message\n",
       level: :warning,
       metadata: [:pid]

config :journey, :graphs, [
  &Journey.Examples.CreditCardApplication.graph/0
]

# Override missed schedules configuration for development
config :journey, :missed_schedules_catchall,
  enabled: true,
  # No hour restriction in dev
  preferred_hour: nil,
  lookback_days: 7
