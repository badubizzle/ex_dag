defmodule ExDagStoreTest do
  use ExUnit.Case
  doctest ExDag

  alias ExDag.DAG.Server
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG
  alias ExDag.DAGRun

  setup do
    priv_dir = :code.priv_dir(:ex_dag)
    path = Path.join([priv_dir, "test_dags"])
    File.rm_rf(path)
    File.mkdir_p(path)
    Application.put_env(:ex_dag, :store_adapter_options, dags_path: path)
  end

  test "save dag" do
    dag_id = "dag1"
    dag = DAG.new(dag_id)

    assert dag.dag_id == dag_id

    :ok = ExDag.Store.save_dag(dag)

    dags = ExDag.Store.get_dags()

    assert Enum.count(dags) == 1
    assert Map.get(dags, dag_id) == dag

    on_exit(fn ->
      :ok
    end)
  end

  test "save dag run" do
    dag_id = "dag1"
    dag = DAG.new(dag_id)

    dag_run = DAGRun.new(dag)

    :ok = ExDag.Store.save_dag_run(dag_run)

    dag_runs = ExDag.Store.get_dag_runs(dag)
    assert Enum.count(dag_runs) == 1
  end

  test "delete dag" do
    dag_id = "dag1"
    dag = DAG.new(dag_id)

    dag_run = DAGRun.new(dag)

    :ok = ExDag.Store.save_dag(dag)
    dags = ExDag.Store.get_dags()
    assert Enum.count(dags) == 1

    :ok = ExDag.Store.delete_dag(dag)
    dags = ExDag.Store.get_dags()
    assert Enum.count(dags) == 0
  end
end
