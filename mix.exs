defmodule ExDag.MixProject do
  @moduledoc false
  use Mix.Project

  def project do
    [
      app: :ex_dag,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {ExDag.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:libgraph, "~> 0.13"},
      {:table_rex, "~> 3.1"},
      {:libcluster, "~> 3.3"},
      {:swarm, "~> 3.4"},
      {:phoenix_pubsub, "~> 2.0"},
      {:jason, "~> 1.4"},
      {:ecto_sql, "~> 3.10"},
      {:postgrex, "~> 0.17"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:husky, "~> 1.0", only: :dev, runtime: false}
    ]
  end
end
