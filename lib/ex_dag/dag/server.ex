defmodule ExDag.DAG.Server do
  @moduledoc """
  Genserver for running tasks in a DAG
  """
  use GenServer

  alias ExDag.DAG
  alias ExDag.DAGRun
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun
  alias ExDag.DAG.Worker

  require Logger

  @server __MODULE__
  @run_interval 5000

  @registry DAGRegister

  # client api

  def run_dag(%DAG{} = dag) do
    case DAG.validate_for_run(dag) do
      true ->
        start_link(dag)

      _ ->
        {:error, :invalid_dag}
    end
  end

  @spec resume_dag(ExDag.DAGRun.t()) :: {:error, :invalid_dag} | {:ok, pid}
  def resume_dag(%DAGRun{dag: dag} = dag_run) do
    # clear all failed tasks
    Logger.info("Resuming dag run: #{dag_run.id}")
    dag = DAG.clear_failed_tasks_runs(dag)

    case DAG.validate_for_run(dag) do
      true ->
        start_link(%DAGRun{dag_run | dag: dag})

      _ ->
        {:error, :invalid_dag}
    end
  end

  def get_dag_run_id(dag_run_id) do
    Swarm.whereis_name({:dag_run, dag_run_id})
  end

  def stop_dag_run(dag_run_id) do
    case Swarm.whereis_name({:dag_run, dag_run_id}) do
      pid when is_pid(pid) ->
        GenServer.stop(pid)

      _ ->
        :not_found
    end
  end

  # server
  def start_link(%DAG{dag_id: dag_id} = args) when is_binary(dag_id) do
    %DAGRun{id: dag_run_id} = dag_run = DAGRun.new(args)

    {:ok, pid} =
      GenServer.start_link(@server, dag_run, name: {:via, :swarm, {:dag_run, dag_run_id}})

    Swarm.join(:dags, pid)
    Swarm.join({:dag_runs, dag_id}, pid)
    {:ok, pid}
  end

  def start_link(%DAGRun{dag: %{dag_id: dag_id}} = dag_run) when is_binary(dag_id) do
    Logger.info("Resuming dag run: #{dag_run.id}")
    dag = DAG.clear_failed_tasks_runs(dag_run.dag)
    dag_run = %DAGRun{dag_run | dag: dag}

    {:ok, pid} =
      GenServer.start_link(@server, dag_run, name: {:via, :swarm, {:dag_run, dag_run.id}})

    Swarm.join(:dags, pid)
    Swarm.join({:dag_runs, dag_id}, pid)
    {:ok, pid}
  end

  # def start_link(%DAG{id: dag_run_id} = args) when is_binary(dag_run_id) do
  #   {:ok, pid} = GenServer.start_link(@server, args, name: {:via, :swarm, {@registry, dag_run_id}})
  #   Swarm.join(:dags, pid)
  #   {:ok, pid}
  # end

  @impl true
  def init(%DAGRun{id: _dag_id} = dag_run) do
    common_init(dag_run)
  end

  defp common_init(dag_run) do
    {:ok, dag_run, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, %DAGRun{dag: %{status: status}} = dag_run) do
    Logger.debug("Starting DAG run")
    dag = dag_run.dag

    if status == DAG.status_init() or status == DAG.status_running() do
      case DAG.validate_for_run(dag) do
        true ->
          schedule_next_run(1000)

          updated_dag = %DAGRun{
            dag_run
            | started_at: DateTime.utc_now(),
              dag: %DAG{dag | status: DAG.status_running()}
          }

          run_on_dag_completed(updated_dag)
          {:noreply, updated_dag}

        _ ->
          {:stop, {:error, :invalid_dag}, dag}
      end
    else
      {:stop, {:error, :invalid_dag_state}, dag}
    end
  end

  @impl true
  def handle_info({:collect, pid, result}, %DAGRun{dag: dag} = dag_run) do
    {task, final_state} =
      case result do
        {:ok, data} ->
          {task, new_state} =
            handled_task_result(dag, pid, nil, data, DAGTask.status_completed(), true)

          timer =
            if is_nil(dag.timer) do
              schedule_next_run()
            else
              dag.timer
            end

          {task,
           {:noreply,
            %DAGRun{dag_run | updated_at: DateTime.utc_now(), dag: %DAG{new_state | timer: timer}}}}

        {:error, error} ->
          {task, new_state} =
            handled_task_result(dag, pid, error, nil, DAGTask.status_failed(), false)

          timer =
            if is_nil(dag.timer) do
              schedule_next_run()
            else
              dag.timer
            end

          {task,
           {:noreply,
            %DAGRun{dag_run | updated_at: DateTime.utc_now(), dag: %DAG{new_state | timer: timer}}}}
      end

    {:noreply, new_run_state} = final_state
    run_on_task_completed(new_run_state, task, result)
    final_state
  end

  def handle_info({:start_next, pid}, %DAGRun{dag: dag} = dag_run) do
    case pid == self() do
      true ->
        %{failed: failed, ready: ready, pending: pending, completed: completed} =
          get_tasks_statuses(dag)

        Logger.debug("Tasks to run: #{Enum.count(ready)}")

        cond do
          Enum.count(failed) > 0 ->
            run_on_dag_completed(dag_run)
            {:stop, {:failed, failed}, dag_run}

          Enum.count(ready) > 0 ->
            Logger.debug("Scheduling tasks: #{inspect(ready)}")
            start_workers(ready, dag_run)

          Enum.count(pending) > 0 ->
            Logger.debug("Tasks pending: #{inspect(pending)}")
            timer = schedule_next_run()
            {:noreply, %DAGRun{dag_run | dag: %DAG{dag | timer: timer}}}

          completed == true ->
            Logger.debug("Completed DAG run")

            dag_run = %DAGRun{
              dag_run
              | updated_at: DateTime.utc_now(),
                ended_at: DateTime.utc_now(),
                dag: %DAG{dag | status: DAG.status_done(), timer: nil}
            }

            run_on_dag_completed(dag_run)
            {:stop, :normal, dag_run}

          true ->
            {:noreply, %DAGRun{dag_run | dag: %DAG{dag | timer: nil}}}
        end
    end
  end

  def handle_info({:EXIT, _pid, :normal}, dag) do
    {:noreply, dag}
  end

  def handle_info({:EXIT, pid, reason}, %DAGRun{dag: dag} = dag_run) do
    {task, new_state} = handled_task_result(dag, pid, reason, nil, DAGTask.status_failed(), false)

    timer =
      if is_nil(dag.timer) do
        schedule_next_run()
      else
        dag.timer
      end

    run_on_task_completed(dag_run, task, reason)
    Phoenix.PubSub.broadcast(ExDag.PubSub, dag.dag_id, {:completed, dag, task, reason})
    {:noreply, %DAGRun{dag_run | dag: %DAG{new_state | timer: timer}}}
  end

  def handle_info({:swarm, :die}, state) do
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
        !DAG.should_run_task(dag, t_id)
      end)

    %{
      running: running_tasks,
      failed: failed,
      ready: tasks_to_run,
      pending: not_ready_to_run,
      completed: Enum.empty?(tasks)
    }
  end

  defp run_on_dag_completed(%DAGRun{dag: %DAG{handler: handler}} = dag_run) do
    Logger.debug("Calling dag completed callback #{inspect(handler)}")

    if function_exported?(handler, :on_dag_completed, 1) do
      apply(handler, :on_dag_completed, [dag_run])
    else
      Logger.debug(fn -> "No dag completed callback found" end)
    end
  end

  defp run_on_task_started(
         %DAGRun{dag: %DAG{handler: handler}} = dag_run,
         %DAGTaskRun{} = task_run
       ) do
    Logger.debug(fn ->
      "Calling task started callback #{task_run.task_id}  #{inspect(handler)}"
    end)

    if function_exported?(handler, :on_task_started, 2) do
      apply(handler, :on_task_started, [dag_run, task_run])
    else
      Logger.debug(fn -> "No task started callback found" end)
    end
  end

  defp run_on_task_completed(%DAGRun{dag: %DAG{handler: handler}} = dag_run, task, result) do
    Logger.debug(fn ->
      "Calling task completed callback #{task.id}  #{inspect(handler)}"
    end)

    if function_exported?(handler, :on_task_completed, 3) do
      apply(handler, :on_task_completed, [dag_run, task, result])
    else
      Logger.debug(fn -> "No dag completed callback found" end)
    end
  end

  defp ready_to_run(%DAGTask{start_date: nil}) do
    true
  end

  defp ready_to_run(%DAGTask{start_date: date}) do
    d = DateTime.diff(date, DateTime.utc_now()) <= 0
    d
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

  defp start_workers([], %DAGRun{dag: dag} = dag_run) do
    {:noreply, %DAGRun{dag_run | dag: %DAG{dag | timer: nil}}}
  end

  defp start_workers([t | rest], %DAGRun{dag: dag} = dag_run) do
    deps = Map.get(dag.task_deps, t, [])
    %DAGTask{} = task = Map.get(dag.tasks, t)

    completed = DAGTask.status_completed()

    completed_deps =
      deps
      |> Enum.map(fn dep_id ->
        case Map.get(dag.tasks, dep_id) do
          %DAGTask{last_run: %DAGTaskRun{status: ^completed} = last_run} ->
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

      tasks =
        Map.put(dag.tasks, t, %DAGTask{
          task
          | status: DAGTask.status_running(),
            last_run: task_run
        })

      dag_run = %DAGRun{
        dag_run
        | updated_at: DateTime.utc_now(),
          dag: %DAG{dag | tasks: tasks, running: running_tasks}
      }

      run_on_task_started(dag_run, task_run)
      start_workers(rest, dag_run)
    else
      unfinished_deps = deps -- completed_deps

      Logger.debug(
        "Cannot start task #{inspect(t)} because deps: #{inspect(unfinished_deps)} are not ready"
      )

      start_workers(rest ++ unfinished_deps, dag_run)
    end
  end

  defp schedule_next_run(interval \\ @run_interval) do
    Process.send_after(self(), {:start_next, self()}, interval)
  end
end
