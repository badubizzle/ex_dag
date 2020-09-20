defmodule ExDag.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      # Starts a worker by calling: ExDag.Worker.start_link(arg)
      # {ExDag.Worker, arg}
      ExDag.DAG.DAGSupervisor,
      {Registry, [keys: :unique, name: DAGRegister]}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ExDag.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
