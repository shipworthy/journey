defmodule Journey.MixProject do
  use Mix.Project

  def project do
    [
      app: :journey,
      version: "0.10.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      test_coverage: [
        summary: [
          threshold: 95
        ]
      ],
      deps: deps()
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
      {:nanoid, "~> 2.1.0"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
