import Config

config :journey, Journey.Repo,
  database: "journey_dev",
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  port: 5438

config :journey, ecto_repos: [Journey.Repo]
