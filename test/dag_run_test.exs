defmodule ExDag.DAGRunTest do
  use ExUnit.Case

  alias ExDag.DAG
  alias ExDag.DAGRun

  test "new creates a DAGRun with a unique id" do
    dag = DAG.new("test_dag")
    dag_run = DAGRun.new(dag)
    assert %DAGRun{} = dag_run
    assert dag_run.dag == dag
    assert is_binary(dag_run.id)
    assert byte_size(dag_run.id) > 0
  end

  test "new sets started_at to current time" do
    dag = DAG.new("test_dag")
    before = DateTime.utc_now()
    dag_run = DAGRun.new(dag)
    end_time = DateTime.utc_now()

    assert DateTime.compare(dag_run.started_at, before) in [:gt, :eq]
    assert DateTime.compare(dag_run.started_at, end_time) in [:lt, :eq]
  end

  test "new sets ended_at to nil" do
    dag = DAG.new("test_dag")
    dag_run = DAGRun.new(dag)
    assert dag_run.ended_at == nil
  end

  test "new sets updated_at to nil" do
    dag = DAG.new("test_dag")
    dag_run = DAGRun.new(dag)
    assert dag_run.updated_at == nil
  end

  test "generate_id returns a string" do
    id = DAGRun.generate_id()
    assert is_binary(id)
    assert byte_size(id) > 0
  end

  test "generate_id returns unique ids" do
    id1 = DAGRun.generate_id()
    id2 = DAGRun.generate_id()
    assert id1 != id2
  end

  test "random_string returns binary of given length" do
    result = DAGRun.random_string(10)
    assert is_binary(result)
    assert byte_size(result) == 10
  end

  test "two dag runs for the same dag have different ids" do
    dag = DAG.new("test_dag")
    run1 = DAGRun.new(dag)
    run2 = DAGRun.new(dag)
    assert run1.id != run2.id
  end
end
