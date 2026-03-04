defmodule ExDagTest do
  use ExUnit.Case
  doctest ExDag

  alias ExDag.DAG.Server
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun
  alias ExDag.DAG

  test "greets the world" do
  end

  test "add dag task" do
    task = DAGTask.new(id: "a", handler: __MODULE__, data: %{op: :+})
    dag = DAG.new("my dag")
    {:ok, dag} = DAG.add_task(dag, task)
    t = DAG.get_task(dag, "a")
    assert t == task
  end

  test "dag id must be non-empty binary" do
    assert {:error, :invalid_dag_id} = DAG.new(id: "a")
    %DAG{} = DAG.new("dag9")
  end

  test "dag task id must be non-empty binary" do
    assert {:error, :invalid_task_id} = DAGTask.new(id: :a, data: %{op: :+})
  end

  test "add invalid dag task should give error" do
    dag = DAG.new("my dag")
    assert {:error, :invalid_task} = DAG.add_task(dag, %DAGTask{id: nil, handler: nil})
  end

  test "adding duplicate tasks should result in error" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    c = DAGTask.new(id: "a", data: %{op: :+})
    dag = DAG.add_task!(dag, a)
    dag = DAG.add_task!(dag, b)
    assert {:error, :task_exists} = DAG.add_task(dag, c)
  end

  test "invalid dag should result error" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b)

    assert {:error, :invalid_dag} == Server.run_dag(dag)
  end

  test "add dag task with parent task" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    assert {:ok, dag} = DAG.add_task(dag, b, "a")

    assert ["b"] == DAG.get_deps(dag, "a")
    assert true == DAG.validate_for_run(dag)
  end

  test "add dag task with no existing parent task should return error" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    b = DAGTask.new(id: "b", data: %{op: :+})
    result = DAG.add_task(dag, b, "c")
    assert result == {:error, :no_parent_task}
    assert DAG.get_tasks(dag) == ["a"]
  end

  test "list last tasks" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    c = DAGTask.new(id: "c", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b, "a")
    {:ok, dag} = DAG.add_task(dag, c, "b")

    last_tasks = DAG.get_last_tasks(dag)
    assert last_tasks == ["a"]
  end

  test "get_tasks returns all task ids" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b)

    tasks = DAG.get_tasks(dag)
    assert Enum.sort(tasks) == ["a", "b"]
  end

  test "set_handler updates the dag handler" do
    dag = DAG.new("my dag")
    updated = DAG.set_handler(dag, __MODULE__)
    assert updated.handler == __MODULE__
  end

  test "set_default_task_handler updates the task_handler" do
    dag = DAG.new("my dag")
    updated = DAG.set_default_task_handler(dag, __MODULE__)
    assert updated.task_handler == __MODULE__
  end

  test "set_tasks_handler updates all task handlers" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b)

    dag = DAG.set_tasks_handler(dag, SomeOtherModule)
    assert Enum.all?(dag.tasks, fn {_, t} -> t.handler == SomeOtherModule end)
  end

  test "validate_for_run returns false for dag with disconnected tasks" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b)
    assert DAG.validate_for_run(dag) == false
  end

  test "validate_for_run returns false for non-dag value" do
    assert DAG.validate_for_run(nil) == false
    assert DAG.validate_for_run("not a dag") == false
  end

  test "get_deps returns empty list when task has no deps" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    assert DAG.get_deps(dag, "a") == []
  end

  test "get_deps_map returns deps map" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b, "a")

    deps_map = DAG.get_deps_map(dag)
    assert is_map(deps_map)
    assert Map.get(deps_map, "a") == ["b"]
  end

  test "get_runs returns empty list for task with no runs" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    assert DAG.get_runs(dag, "a") == []
  end

  test "status_running returns :running" do
    assert DAG.status_running() == :running
  end

  test "status_done returns :done" do
    assert DAG.status_done() == :done
  end

  test "status_init returns :init" do
    assert DAG.status_init() == :init
  end

  test "completed? returns false when tasks are not completed" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    assert DAG.completed?(dag) == false
  end

  test "completed? returns true when all last tasks are completed" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    dag = %DAG{dag | tasks: Map.put(dag.tasks, "a", %DAGTask{dag.tasks["a"] | status: :completed})}
    assert DAG.completed?(dag) == true
  end

  test "get_completed_tasks returns completed tasks" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b)
    dag = %DAG{dag | tasks: Map.put(dag.tasks, "a", %DAGTask{dag.tasks["a"] | status: :completed})}

    completed = DAG.get_completed_tasks(dag)
    assert Enum.count(completed) == 1
    assert {"a", %DAGTask{status: :completed}} = hd(completed)
  end

  test "get_pending_tasks returns pending tasks" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b)
    dag = %DAG{dag | tasks: Map.put(dag.tasks, "a", %DAGTask{dag.tasks["a"] | status: :completed})}

    pending = DAG.get_pending_tasks(dag)
    assert Enum.count(pending) == 1
    assert {"b", %DAGTask{status: nil}} = hd(pending)
  end

  test "get_running_tasks returns running tasks" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b)
    dag = %DAG{dag | tasks: Map.put(dag.tasks, "a", %DAGTask{dag.tasks["a"] | status: :running})}

    running = DAG.get_running_tasks(dag)
    assert Enum.count(running) == 1
    assert {"a", %DAGTask{status: :running}} = hd(running)
  end

  test "sorted_tasks returns tasks in dependency order" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    b = DAGTask.new(id: "b", data: %{op: :+})
    c = DAGTask.new(id: "c", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    {:ok, dag} = DAG.add_task(dag, b, "a")
    {:ok, dag} = DAG.add_task(dag, c, "b")

    sorted = DAG.sorted_tasks(dag)
    assert is_map(sorted)
    assert Enum.sort(Map.keys(sorted)) == ["a", "b", "c"]
  end

  test "should_run_task returns true for task without a last run" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, a)
    assert DAG.should_run_task(dag, "a") == true
  end

  test "should_run_task returns false for failed task with stop_on_failure" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+}, stop_on_failure: true)
    {:ok, dag} = DAG.add_task(dag, a)

    task_run = %DAGTaskRun{task: dag.tasks["a"], task_id: "a", status: :failed}
    dag = %DAG{dag | tasks: Map.put(dag.tasks, "a", %DAGTask{dag.tasks["a"] | last_run: task_run})}
    dag = %DAG{dag | task_runs: Map.put(dag.task_runs, "a", [task_run])}

    assert DAG.should_run_task(dag, "a") == false
  end

  test "clear_failed_tasks_runs resets last_run on failed tasks" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    a = DAGTask.new(id: "a", data: %{op: :+}, stop_on_failure: true)
    {:ok, dag} = DAG.add_task(dag, a)

    task_run = %DAGTaskRun{task: dag.tasks["a"], task_id: "a", status: :failed}
    dag = %DAG{dag | tasks: Map.put(dag.tasks, "a", %DAGTask{dag.tasks["a"] | last_run: task_run})}
    dag = %DAG{dag | task_runs: Map.put(dag.task_runs, "a", [task_run])}

    cleared = DAG.clear_failed_tasks_runs(dag)
    assert cleared.tasks["a"].last_run == nil
  end

  test "to_string on dag returns a string" do
    dag = DAG.new("my dag")
    result = to_string(dag)
    assert is_binary(result)
    assert String.contains?(result, "DAG")
  end

  test "add task using keyword list with parent" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    {:ok, dag} = DAG.add_task(dag, id: "a", data: %{op: :+})
    {:ok, dag} = DAG.add_task(dag, id: "b", data: %{op: :+}, parent: "a")
    assert DAG.get_deps(dag, "a") == ["b"]
  end

  test "add task using keyword list without handler uses default handler" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    {:ok, dag} = DAG.add_task(dag, id: "a", data: %{op: :+})
    assert dag.tasks["a"].handler == __MODULE__
  end

  test "add task using keyword list with explicit handler uses that handler" do
    dag =
      DAG.new("my dag")
      |> DAG.set_default_task_handler(__MODULE__)

    {:ok, dag} = DAG.add_task(dag, id: "a", data: %{op: :+}, handler: SomeOtherModule)
    assert dag.tasks["a"].handler == SomeOtherModule
  end

  test "add_task! raises on error" do
    dag = DAG.new("my dag")

    assert catch_throw(DAG.add_task!(dag, %DAGTask{id: nil, handler: nil})) ==
             {:error, :invalid_task}
  end

  test "new dag with handler and task_handler" do
    dag = DAG.new("my dag", __MODULE__, __MODULE__)
    assert dag.handler == __MODULE__
    assert dag.task_handler == __MODULE__
  end

  test "new dag with nil dag_id returns error" do
    assert {:error, :invalid_dag_id} = DAG.new(nil)
  end
end
