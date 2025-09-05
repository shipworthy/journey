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
config :journey, :stalled_executions_sweep,
  enabled: true,
  # Hour of day (0-23, UTC) when sweep should run, nil for no restriction
  preferred_hour: 22

import_config "#{config_env()}.exs"
