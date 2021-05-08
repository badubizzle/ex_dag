import Config

config :ex_dag, ExDag.Repo,
  database: "ex_dag_repo",
  username: "badu",
  password: "",
  hostname: "localhost"

config :ex_dag,
  store_adapter: ExDag.Store.FileStore,
  store_adapter_options: [dags_path: "priv/dags"]

config :ex_dag,
  ecto_repos: [ExDag.Repo]

if Mix.env() == :dev do
  config :mix_test_watch,
    clear: true,
    tasks: [
      "test",
      "format",
      "credo explain"
    ]

  config :husky,
    pre_commit: "mix format && mix credo",
    pre_push: "mix format --check-formatted && mix credo && mix test"
end
