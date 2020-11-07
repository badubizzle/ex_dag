defmodule ExDag.DAG.Handlers.DAGHandler do
  alias ExDag.DAGRun
  alias ExDag.DAG.DAGTask

  @callback on_task_completed(DAGRun.t(), DAGTask.t(), any()) :: any()
  @callback on_dag_completed(DAGRun.t()) :: any()

end
