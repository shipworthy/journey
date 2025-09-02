import Config

config :journey, Journey.Repo,
  database: "journey_repo",
  username: "user",
  password: "pass",
  hostname: "localhost"

config :journey, ecto_repos: [Journey.Repo]

# Missed schedules catch-all sweep configuration
config :journey, :missed_schedules_catchall,
  enabled: true,
  # Hour of day (0-23, UTC) when sweep should run, nil for no restriction
  preferred_hour: 2,
  # Number of days to look back for missed schedules
  lookback_days: 7

import_config "#{config_env()}.exs"
