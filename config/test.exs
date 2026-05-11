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
       metadata: [:pid, :mfa]

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

# Lower heartbeat floors for tests so heartbeat behavior can be exercised in seconds
# rather than tens of seconds. Production defaults (30 / 10) remain in effect outside :test.
config :journey, :min_heartbeat_interval_seconds, 1
config :journey, :heartbeat_deadline_buffer_seconds, 1

# Shorten the post-error retry jitter so error-path tests don't spend most of their wall
# time in `Process.sleep(:rand.uniform(10_000))`. Production default (10_000 ms) is set in
# `Journey.Scheduler` and is unchanged outside :test.
config :journey, :compute_error_jitter_max_ms, 100
