defmodule Journey.MixProject do
  use Mix.Project

  def project do
    [
      app: :journey,
      version: "0.0.2",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: description(),
      dialyzer_cache_directory: "priv/dialzer_cache",
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer_otp24_elixir1.12.1.plt"}
      ],
      # Docs
      name: "Journey",
      source_url: "https://github.com/shipworthy/journey",
      docs: [
        # The main page in the docs
        main: "Journey",
        # logo: "path/to/logo.png",
        extras: ["README.md", "LICENSE"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Journey.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp description() do
    "Journey simplifies writing and running workflows."
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:docception, "~> 0.4.1", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false},
      {:wait_for_it, "~> 1.3.0", only: :test, runtime: false}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end

  def package do
    [
      name: "journey",
      # These are the default files included in the package
      # files: ~w(lib .formatter.exs mix.exs README*  LICENSE*),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/shipworthy/journey"}
    ]
  end
end
