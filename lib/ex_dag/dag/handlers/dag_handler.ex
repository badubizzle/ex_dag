defmodule ExDag.DAG.Handlers.DAGHandler do
  @moduledoc """
  DAG handler interface/behavior
  """
  alias ExDag.DAGRun
  alias ExDag.DAG.DAGTask

  @callback on_task_completed(dag_run :: DAGRun.t(), task :: DAGTask.t(), result :: any()) ::
              any()
  @callback on_dag_completed(dag_run :: DAGRun.t()) :: any()
end
