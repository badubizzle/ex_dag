defmodule ExDag.DAG.DAGTaskTest do
  use ExUnit.Case

  alias ExDag.DAG.DAGTask

  test "new task with valid id returns task struct" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{})
    assert %DAGTask{id: "task1"} = task
  end

  test "new task with non-binary id returns error" do
    assert {:error, :invalid_task_id} = DAGTask.new(id: :not_a_binary, data: %{})
    assert {:error, :invalid_task_id} = DAGTask.new(id: 123, data: %{})
    assert {:error, :invalid_task_id} = DAGTask.new(id: nil, data: %{})
  end

  test "new task with empty binary id returns error" do
    assert {:error, :invalid_task_id} = DAGTask.new(id: "", data: %{})
  end

  test "validate returns true for valid task" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{op: :+})
    assert DAGTask.validate(task) == true
  end

  test "validate returns false when handler is nil" do
    task = %DAGTask{id: "task1", handler: nil, data: %{}}
    assert DAGTask.validate(task) == false
  end

  test "validate returns false when data is nil" do
    task = %DAGTask{id: "task1", handler: __MODULE__, data: nil}
    assert DAGTask.validate(task) == false
  end

  test "validate returns false when id is nil" do
    task = %DAGTask{id: nil, handler: __MODULE__, data: %{}}
    assert DAGTask.validate(task) == false
  end

  test "is_pending returns true for new task" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{})
    assert DAGTask.is_pending(task) == true
  end

  test "is_pending returns false when task has a status" do
    task = %DAGTask{id: "task1", status: :running}
    assert DAGTask.is_pending(task) == false
  end

  test "is_completed returns true when task status is completed" do
    task = %DAGTask{id: "task1", status: :completed}
    assert DAGTask.is_completed(task) == true
  end

  test "is_completed returns false when task is not completed" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{})
    assert DAGTask.is_completed(task) == false
  end

  test "is_running returns true when task status is running" do
    task = %DAGTask{id: "task1", status: :running}
    assert DAGTask.is_running(task) == true
  end

  test "is_running returns false when task is not running" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{})
    assert DAGTask.is_running(task) == false
  end

  test "status_completed returns :completed" do
    assert DAGTask.status_completed() == :completed
  end

  test "status_failed returns :failed" do
    assert DAGTask.status_failed() == :failed
  end

  test "status_running returns :running" do
    assert DAGTask.status_running() == :running
  end

  test "set_handler updates handler on task" do
    task = DAGTask.new(id: "task1", handler: __MODULE__, data: %{})
    updated = DAGTask.set_handler(task, SomeModule)
    assert updated.handler == SomeModule
  end
end
