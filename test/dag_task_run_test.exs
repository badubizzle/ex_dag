defmodule ExDag.DAG.DAGTaskRunTest do
  use ExUnit.Case

  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun

  test "new creates a DAGTaskRun with status running" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{op: :+})
    task_run = DAGTaskRun.new(task, %{}, self())

    assert %DAGTaskRun{} = task_run
    assert task_run.task_id == "task1"
    assert task_run.task == task
    assert task_run.handler == __MODULE__
    assert task_run.status == :running
    assert task_run.payload == %{}
    assert task_run.collector_pid == self()
  end

  test "new sets started_at to current time" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{op: :+})
    before = DateTime.utc_now()
    task_run = DAGTaskRun.new(task, %{}, self())
    end_time = DateTime.utc_now()

    assert DateTime.compare(task_run.started_at, before) in [:gt, :eq]
    assert DateTime.compare(task_run.started_at, end_time) in [:lt, :eq]
  end

  test "new sets result and error to nil" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{op: :+})
    task_run = DAGTaskRun.new(task, %{}, self())
    assert task_run.result == nil
    assert task_run.error == nil
    assert task_run.ended_at == nil
  end

  test "new stores payload" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{op: :+})
    payload = %{"dep_task" => 42}
    task_run = DAGTaskRun.new(task, payload, self())
    assert task_run.payload == payload
  end
end
