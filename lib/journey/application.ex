defmodule Journey.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: Journey.Worker.start_link(arg)
      # {Journey.Worker, arg}
      Journey.Repo,
      {Ecto.Migrator, repos: [Journey.Repo]},
      Journey.Graph.Catalog
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Journey.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
