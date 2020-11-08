defmodule ExDag.DAG.Utils.DAGHandler do
  @moduledoc """
  Sample implementation of DAG handler behaviour
  """

  @behaviour ExDag.DAG.Handlers.DAGHandler

  @impl true
  def on_task_completed(dag_run, _task, _result) do
    ExDag.DAG.Utils.print_status(dag_run.dag)
    ExDag.DAG.Utils.print_task_runs(dag_run.dag.task_runs)
  end

  @impl true
  def on_dag_completed(dag_run) do
    ExDag.DAG.Utils.print_status(dag_run.dag)
    ExDag.DAG.Utils.print_task_runs(dag_run.dag.task_runs)
  end
end
