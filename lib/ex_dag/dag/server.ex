defmodule ExDag.DAG.Server do
  @moduledoc """
  Genserver for running tasks in a DAG
  """
  use GenServer

  alias ExDag.DAG
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun
  alias ExDag.DAG.Worker

  require Logger

  @server __MODULE__
  @run_interval 5000
  @status_init :init
  @status_running :running

  @registry DAGRegister

  # client api

  def run_dag(%DAG{} = dag) do
    IO.inspect(dag)
    case DAG.validate_for_run(dag) do
      true ->
        start_link(dag)

      _ ->
        :invalid_dag
    end
  end

  def get_dag_id(dag_id) do
    Swarm.whereis_name({:dag, dag_id})
  end

  def stop_dag(dag_id) do

    case Swarm.whereis_name({:dag, dag_id}) do
      pid when is_pid(pid) ->
        GenServer.stop(pid)
    end
    # case Registry.lookup(@registry, dag_id) do
    #   [{pid, _}] ->
    #     GenServer.stop(pid)

    #   _ ->
    #     :error
    # end
  end

  # server
  def start_link(%{dag_id: dag_id} = args) when is_binary(dag_id) do
    # name = dag_id
    #  = Swarm.register_name(name, __MODULE__, :register, [name])

    # {:ok, pid}
    {:ok, pid} = GenServer.start_link(@server, args, name: {:via, :swarm, {:dag, dag_id}})
    Swarm.join(:dags, pid)
    {:ok, pid}
  end

  def start_link(%DAG{dag_id: dag_id} = args) when is_binary(dag_id) do
    {:ok, pid} = GenServer.start_link(@server, args, name: {:via, :swarm, {@registry, dag_id}})
    # name = dag_id
    # {:ok, pid} = Swarm.register_name(name, __MODULE__, :register, [name])
    Swarm.join(:dags, pid)
    {:ok, pid}
  end

  @impl true
  def init(%DAG{dag_id: _dag_id} = dag) do
    common_init(dag)
  end

  def init(%{dag_id: dag_id}) do
    dag = DAG.new(dag_id)
    common_init(dag)
  end

  defp common_init(dag) do
    {:ok, dag, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, %DAG{timer: nil, status: @status_init} = dag) do
    Logger.debug("Starting DAG run")

    case DAG.validate_for_run(dag) do
      true ->
        schedule_next_run(1000)
        {:noreply, %DAG{dag | status: @status_running}}

      _ ->
        {:stop, {:error, :invalid_dag}, dag}
    end
  end

  @impl true
  def handle_info({:collect, pid, result}, dag) do
    {task, final_state} =
      case result do
        {:ok, data} ->
          {task, new_state} = handled_task_result(dag, pid, nil, data, :completed, true)

          timer =
            if is_nil(dag.timer) do
              schedule_next_run()
            else
              dag.timer
            end

          {task, {:noreply, %DAG{new_state | timer: timer}}}

        {:error, error} ->
          {task, new_state} = handled_task_result(dag, pid, error, nil, :failed, false)

          timer =
            if is_nil(dag.timer) do
              schedule_next_run()
            else
              dag.timer
            end

          {task, {:noreply, %DAG{new_state | timer: timer}}}
      end

    {:noreply, new_state} = final_state
    run_on_task_completed(new_state, task, result)
    final_state
  end

  def handle_info({:start_next, pid}, %DAG{} = dag) do
    case pid == self() do
      true ->
        %{failed: failed, ready: ready, pending: pending, completed: completed} =
          get_tasks_statuses(dag)

        Logger.debug("Tasks to run: #{Enum.count(ready)}")

        cond do
          Enum.count(failed) > 0 ->
            {:stop, {:failed, failed}, dag}

          Enum.count(ready) > 0 ->
            Logger.debug("Scheduling tasks: #{inspect(ready)}")
            new_state = start_workers(ready, dag)
            {:noreply, %DAG{new_state | timer: nil}}

          Enum.count(pending) > 0 ->
            Logger.debug("Tasks pending: #{inspect(pending)}")
            timer = schedule_next_run()
            {:noreply, %DAG{dag | timer: timer}}

          completed == true ->
            Logger.debug("Completed DAG run")
            dag = %DAG{dag | status: :done, timer: nil}
            # run_on_task_completed(dag, nil, nil)
            run_on_dag_completed(dag)
            {:stop, :normal, dag}

          true ->
            {:noreply, %DAG{dag | timer: nil}}
        end
    end
  end

  def handle_info({:EXIT, _pid, :normal}, dag) do
    {:noreply, dag}
  end

  def handle_info({:EXIT, pid, reason}, %DAG{} = dag) do
    {task, new_state} = handled_task_result(dag, pid, reason, nil, :failed, false)

    timer =
      if is_nil(dag.timer) do
        schedule_next_run()
      else
        dag.timer
      end

    run_on_task_completed(dag, task, reason)
    Phoenix.PubSub.broadcast(ExDag.PubSub, dag.dag_id, {:completed, dag, task, reason})
    {:noreply, %DAG{new_state | timer: timer}}
  end

   def handle_info({:swarm, :die}, state) do
    IO.inspect(state, label: "swarm die")
    {:stop, :shutdown, state}
  end

  # private functions

  def get_tasks_statuses(%DAG{} = dag) do
    tasks =
      for v <- Graph.vertices(dag.g), Graph.out_degree(dag.g, v) == 0 do
        v
      end

    running_task_ids = Map.values(dag.running)

    running_tasks =
      Enum.filter(dag.tasks, fn {t_id, _} ->
        Enum.member?(running_task_ids, t_id)
      end)
      |> Enum.map(fn {_, t} ->
        t
      end)

    tasks_to_run =
      tasks
      |> Enum.filter(fn t_id ->
        !Enum.member?(running_task_ids, t_id)
      end)

    not_ready_to_run =
      tasks_to_run
      |> Enum.filter(fn t_id ->
        !ready_to_run(Map.get(dag.tasks, t_id))
      end)

    tasks_to_run =
      tasks_to_run
      |> Enum.filter(fn t_id ->
        !Enum.member?(not_ready_to_run, t_id)
      end)

    failed =
      tasks_to_run
      |> Enum.filter(fn t_id ->
        !should_run_task(dag, t_id)
      end)

    %{
      running: running_tasks,
      failed: failed,
      ready: tasks_to_run,
      pending: not_ready_to_run,
      completed: Enum.count(tasks) == 0
    }
  end

  defp run_on_dag_completed(%DAG{}=dag) do
    Logger.debug("Calling dag completed callback #{inspect(dag.on_dag_completed)}")

    case dag.on_dag_completed do
      {m, f, _a} ->
        spawn(fn ->
          apply(m, f, [dag])
        end)

      {m, f} ->
        spawn(fn ->
          apply(m, f, [dag])
        end)

      f when is_function(f, 1) ->
        spawn(fn ->
          f.(dag)
        end)

      _ ->
        Logger.debug(fn -> "No dag completed callback found" end)
        :ok
    end

  end
  defp run_on_task_completed(dag, task, result) do
    Logger.debug(fn ->
      "Calling task completed callback #{task.id}"
    end)

    case dag.on_task_completed do
      {m, f, _a} ->
        spawn(fn ->
          apply(m, f, [dag, task, result])
        end)

      {m, f} ->
        spawn(fn ->
          apply(m, f, [dag, task, result])
        end)

      f when is_function(f, 3) ->
        spawn(fn ->
          f.(dag, task, result)
        end)

      _ ->
        Logger.debug(fn -> "No task completed callback found" end)
        :ok
    end
  end

  defp ready_to_run(%DAGTask{start_date: nil}) do
    true
  end

  defp ready_to_run(%DAGTask{start_date: date}) do
    d = DateTime.diff(date, DateTime.utc_now()) <= 0
    d
  end

  defp should_run_task(%DAG{} = dag, task_id) do
    %DAGTask{last_run: last_run} = task = Map.get(dag.tasks, task_id)

    case last_run do
      %DAGTaskRun{status: :failed} ->
        if task.stop_on_failure do
          false
        else
          task_runs = Map.get(dag.task_runs, task_id)
          task.retries && Enum.count(task_runs) < task.retries
        end

      _ ->
        true
    end
  end

  defp handled_task_result(%DAG{} = dag, pid, error, result, status, remove_task) do
    %DAGTaskRun{task_id: task_id} = task_run = Map.get(dag.running, pid)

    task_run = %DAGTaskRun{
      task_run
      | error: error,
        result: result,
        status: status,
        ended_at: DateTime.utc_now()
    }

    task_run = %DAGTaskRun{task_run | status: status}

    task_runs = Map.get(dag.task_runs, task_id, [])
    task_runs = Map.put(dag.task_runs, task_id, [task_run | task_runs])

    task = Map.get(dag.tasks, task_id)

    tasks = Map.put(dag.tasks, task_id, %DAGTask{task | status: status, last_run: task_run})
    running = Map.delete(dag.running, pid)

    g =
      if remove_task do
        Graph.delete_vertex(dag.g, task_id)
      else
        dag.g
      end

    {task, %DAG{dag | g: g, task_runs: task_runs, running: running, tasks: tasks}}
  end

  defp start_workers([], %DAG{} = dag) do
    dag
  end

  defp start_workers([t | rest], %DAG{} = dag) do
    deps = Map.get(dag.task_deps, t, [])
    task = Map.get(dag.tasks, t)

    completed_deps =
      deps
      |> Enum.map(fn dep_id ->
        case Map.get(dag.tasks, dep_id) do
          %DAGTask{last_run: %DAGTaskRun{status: :completed} = last_run} ->
            {dep_id, last_run.result}

          _ ->
            nil
        end
      end)
      |> Enum.filter(&(!is_nil(&1)))

    if Enum.count(deps) == Enum.count(completed_deps) do
      Process.flag(:trap_exit, true)
      task_run = DAGTaskRun.new(task, Map.new(completed_deps), self())
      {:ok, pid} = Worker.run_task(task_run)
      running_tasks = Map.put(dag.running, pid, task_run)
      task = Map.get(dag.tasks, t)
      tasks = Map.put(dag.tasks, t, %DAGTask{task | status: :running, last_run: task_run})
      start_workers(rest, %DAG{dag | tasks: tasks, running: running_tasks})
    else
      # IO.puts("Cannot start task #{inspect(t)} because deps is not ready")
      start_workers(rest, dag)
    end
  end

  defp schedule_next_run(interval \\ @run_interval) do
    Process.send_after(self(), {:start_next, self()}, interval)
  end
end
