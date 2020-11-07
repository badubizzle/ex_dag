import Config

config :ex_dag, ExDag.Repo,
  database: "ex_dag_repo",
  username: "badu",
  password: "",
  hostname: "localhost"

config :ex_dag,
  ecto_repos: [ExDag.Repo]
