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

# Overrides for the "missed_schedules_catchall" sweep
# (to pick up scheduled computations that came due while the system was down).
config :journey, :missed_schedules_catchall,
  enabled: true,
  # No hour restriction in dev
  preferred_hour: nil,
  lookback_days: 7

# Overrides for the "stalled_executions" sweep
# (to pick up executions that may have stalled due to crashes/power loss)
config :journey, :stalled_executions_sweep, enabled: true
