defmodule Journey.MixProject do
  use Mix.Project

  @version "0.10.26"

  def project do
    [
      app: :journey,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      start_permanent: Mix.env() == :prod,
      name: "Journey",
      docs: [
        main: "Journey",
        extras: ["README.md", "LICENSE.md", "lib/examples/basic.livemd"]
      ],
      test_coverage: [
        summary: [
          threshold: 79
        ]
      ],
      deps: deps()
    ]
  end

  def package do
    [
      name: "journey",
      description:
        "Journey is a library for defining and running self-computing dataflow graphs with persistence, reliability, and scalability.",
      licenses: ["Journey License"],
      links: %{
        "GitHub" => "https://github.com/markmark206/journey",
        "License" => "https://github.com/markmark206/journey/blob/v#{@version}/LICENSE.md",
        "About" => "https://gojourney.dev"
      }
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Journey.Application, []}
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test_load", "test/support"]
  defp elixirc_paths(_), do: ["lib", "test_load"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.7.12", only: [:dev, :test], runtime: false},
      {:ecto, "~> 3.12.5"},
      {:ecto_sql, "~> 3.12.1"},
      {:ex_doc, "~> 0.38.2", only: :dev, runtime: false},
      {:keyword_validator, "~> 2.1.0"},
      {:mix_test_watch, "~> 1.3", only: [:dev, :test], runtime: false},
      {:nanoid, "~> 2.1.0"},
      {:number, "~> 1.0.5"},
      {:parent, "~> 0.12.1"},
      {:postgrex, "~> 0.20 or ~> 0.21"},
      {:wait_for_it, "~> 2.1", only: [:dev, :test], runtime: false}

      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
