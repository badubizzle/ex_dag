defmodule ExDag.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_dag,
      version: "0.1.0",
      elixir: "~> 1.10",
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
      {:libgraph, "~> 0.7"},
      {:table_rex, "~> 3.0.0"},
      {:libcluster, "~> 3.2"},
      {:swarm, "~> 3.0"},
      {:phoenix_pubsub, "~> 2.0"},
      {:jason, "~> 1.2"},
      {:ecto_sql, "~> 3.0"},
      {:postgrex, ">= 0.0.0"}
    ]
  end
end
