defmodule ExDag.Repo do
  use Ecto.Repo,
    otp_app: :ex_dag,
    adapter: Ecto.Adapters.Postgres
end
