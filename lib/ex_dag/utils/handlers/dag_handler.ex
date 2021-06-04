defmodule ExDag.DAG.Utils.DAGHandler do
  @moduledoc """
  Sample implementation of DAG handler behaviour
  """
  alias ExDag.DAG.Utils
  alias ExDag.DAG.DAGTaskRun
  alias ExDag.DAGRun
  alias ExDag.DAG
  alias ExDag.Store

  require Logger

  @behaviour ExDag.DAG.Handlers.DAGHandler

  @impl true
  def on_task_completed(dag_run, task, result) do
    Logger.debug("Task completed callback: #{inspect(task.id)} Result: #{inspect(result)}")
    Utils.print_status(dag_run.dag)
    Utils.print_task_runs(dag_run.dag.task_runs)
    Store.save_dag_run(dag_run)
  end

  @impl true
  def on_dag_completed(dag_run) do
    Logger.debug("DAG completed callback: #{inspect(dag_run.id)}")
    Utils.print_status(dag_run.dag)
    Utils.print_task_runs(dag_run.dag.task_runs)
    Store.save_dag_run(dag_run)
  end

  @impl true
  def on_task_started(
        %DAGRun{dag: %DAG{task_runs: task_runs} = dag} = dag_run,
        %DAGTaskRun{task_id: task_id} = _task_run
      ) do
    Logger.debug("Task started callback: #{inspect(task_id)}")
    Utils.print_status(dag)
    Utils.print_task_runs(task_runs)
    Store.save_dag_run(dag_run)
  end
end
