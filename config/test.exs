import Config

config :journey, Journey.Repo,
  database: "journey_test",
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  queue_target: 10_000,
  queue_interval: 20_000,
  timeout: 60_000,
  pool_size: 60,
  pool: Ecto.Adapters.SQL.Sandbox,
  ownership_timeout: 700_000,
  port: 5432

config :journey, ecto_repos: [Journey.Repo]

config :logger,
       :console,
       format: "$time [$level] $metadata$message\n",
       level: :warning,
       metadata: [:pid]

config :journey, :graphs, [
  &Journey.Test.Support.create_test_graph1/0,
  fn -> Journey.Test.Support.create_test_graph2() end
]

# Overrides for the "missed_schedules_catchall" sweep
# (to pick up scheduled computations that came due while the system was down).
config :journey, :missed_schedules_catchall,
  enabled: true,
  # No hour restriction in tests
  preferred_hour: nil,
  lookback_days: 7

# Overrides for the "stalled_executions" sweep
# (to pick up executions that may have stalled due to crashes/power loss)
config :journey, :stalled_executions_sweep, enabled: true
