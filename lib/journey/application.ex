defmodule Journey.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  require Logger
  use Application

  @impl true
  def start(_type, _args) do
    Logger.error("Starting Journey application")

    if log_level = Application.get_env(:journey, :log_level) do
      Logger.put_application_level(:journey, log_level)
    end

    children = [
      # Starts a worker by calling: Journey.Worker.start_link(arg)
      # {Journey.Worker, arg}
      Journey.Repo,
      {Ecto.Migrator, repos: [Journey.Repo]},
      Journey.Graph.Catalog,
      {Task, fn -> initialize_graphs() end},
      Journey.Scheduler.Background.Periodic
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Journey.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp initialize_graphs() do
    for f_factory <- Application.get_env(:journey, :graphs, []) do
      graph = f_factory.() |> Journey.Graph.Catalog.register()
      Logger.info("Registering graph #{inspect(graph.name)} from config")
    end

    registered_graphs = Journey.GraphRegistry.all_registered_graphs()
    Logger.error("Found #{length(registered_graphs)} registered graphs")

    for {module, func_name} <- registered_graphs do
      graph = apply(module, func_name, []) |> Journey.Graph.Catalog.register()
      Logger.error("Registering graph #{inspect(graph.name)} from @registered_graph in #{inspect(module)}")
    end
  end
end
