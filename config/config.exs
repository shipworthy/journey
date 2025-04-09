import Config

config :journey, Journey.Repo,
  database: "journey_repo",
  username: "user",
  password: "pass",
  hostname: "localhost"

# config :journey, Journey.Repo,
#  database: "journey_repo",
#  username: "user",
#  password: "pass",
#  hostname: "localhost"

config :journey, ecto_repos: [Journey.Repo]

import_config "#{config_env()}.exs"
