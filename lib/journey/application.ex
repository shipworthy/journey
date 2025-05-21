defmodule Journey.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  require Logger
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: Journey.Worker.start_link(arg)
      # {Journey.Worker, arg}
      Journey.Repo,
      {Ecto.Migrator, repos: [Journey.Repo]},
      Journey.Graph.Catalog,
      {Task, fn -> initialize_graphs() end},
      Journey.Scheduler.BackgroundSweeps
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Journey.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp initialize_graphs() do
    for f_factory <- Application.get_env(:journey, :graphs, []) do
      graph = f_factory.() |> Journey.Graph.Catalog.register()
      Logger.info("Registering graph #{inspect(graph.name)}")
    end
  end
end
