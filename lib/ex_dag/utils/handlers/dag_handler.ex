defmodule ExDag.DAG.Utils.DAGHandler do
  @moduledoc """
  Sample implementation of DAG handler behaviour
  """
  alias ExDag.DAG.Utils
  alias ExDag.Store

  @behaviour ExDag.DAG.Handlers.DAGHandler

  @impl true
  def on_task_completed(dag_run, _task, _result) do
    Utils.print_status(dag_run.dag)
    Utils.print_task_runs(dag_run.dag.task_runs)
    Store.save_dag_run(dag_run)
  end

  @impl true
  def on_dag_completed(dag_run) do
    Utils.print_status(dag_run.dag)
    Utils.print_task_runs(dag_run.dag.task_runs)
    Store.save_dag_run(dag_run)
  end
end
