defmodule ExDag.DAG.Handlers.TaskHandler do
  alias ExDag.DAGRun
  alias ExDag.DAG.DAGTask
  @callback on_success(DAGRun.t(), DAGTask.t()) :: any()

  @callback run_task(DAGTask.t(), any()) :: any()
end
