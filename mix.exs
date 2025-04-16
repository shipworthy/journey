defmodule Journey.MixProject do
  use Mix.Project

  def project do
    [
      app: :journey,
      version: "0.10.0",
      elixir: "~> 1.18",
      package: package(),
      start_permanent: Mix.env() == :prod,
      name: "Journey",
      docs: [
        main: "readme",
        extras: ["README.md", "LICENSE"]
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
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/markmark206/journey"}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Journey.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ecto, "~> 3.12.5"},
      {:ecto_sql, "~> 3.12.1"},
      {:ex_doc, "~> 0.37", only: :dev, runtime: false},
      {:nanoid, "~> 2.1.0"},
      {:parent, "~> 0.12.1"},
      {:postgrex, "~> 0.20.0"}

      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
