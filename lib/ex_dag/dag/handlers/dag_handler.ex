defmodule ExDag.DAG.Handlers.DAGHandler do
  @moduledoc """
  DAG handler interface/behavior
  """
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun
  alias ExDag.DAGRun

  @callback on_task_completed(dag_run :: DAGRun.t(), task :: DAGTask.t(), result :: any()) ::
              any()
  @callback on_dag_completed(dag_run :: DAGRun.t()) :: any()

  @callback on_task_started(dag_run :: DAGRun.t(), task_run :: DAGTaskRun.t()) :: any()
end
