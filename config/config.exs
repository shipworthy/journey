import Config

config :journey, Journey.Repo,
  database: "journey_repo",
  username: "user",
  password: "pass",
  hostname: "localhost"

config :journey, ecto_repos: [Journey.Repo]

# The sweep to pick up scheduled computations that came due while the system was down.
config :journey, :missed_schedules_catchall,
  enabled: true,
  # Hour of day (0-23, UTC) when sweep should run, nil for no restriction
  preferred_hour: 20,
  # Number of days to look back for scheduled computations to pick up.
  lookback_days: 7

# The sweep to pick up executions that may have stalled
config :journey, :stalled_executions_sweep, enabled: true

# The sweep to clean up abandoned computations
config :journey, :abandoned_sweep,
  # Minimum seconds between sweep runs (default: 59 seconds)
  min_seconds_between_runs: 119,
  enabled: true

# The sweep to find and compute unblocked schedule nodes
config :journey, :schedule_nodes_sweep,
  # Minimum seconds between sweep runs (default: 120 seconds / 2 minutes)
  min_seconds_between_runs: 59,
  enabled: true

# Background sweeper configuration
config :journey, :background_sweeper,
  # Period in seconds between sweep runs (default: 60 seconds)
  period_seconds: 60

import_config "#{config_env()}.exs"
