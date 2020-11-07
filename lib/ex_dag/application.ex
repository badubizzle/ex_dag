defmodule ExDag.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do

    topologies = [
      chat: [
        strategy: Cluster.Strategy.Gossip
      ]
    ]

    children = [
      # Starts a worker by calling: ExDag.Worker.start_link(arg)
      {Phoenix.PubSub, name: ExDag.PubSub},
      {Cluster.Supervisor, [topologies, [name: ExDag.ClusterSupervisor]]},
      {ExDag.Tracker, [name: ExDag.Tracker, pubsub_server: ExDag.PubSub]},
      ExDag.DAG.DAGSupervisor
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ExDag.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
