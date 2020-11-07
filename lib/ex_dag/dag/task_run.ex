defmodule ExDag.DAG.DAGTaskRun do
  alias ExDag.DAG.DAGTask

  @enforce_keys [:task_id, :task]
  defstruct task: nil,
            task_id: nil,
            handler: nil,
            payload: nil,
            result: nil,
            error: nil,
            status: nil,
            started_at: nil,
            ended_at: nil,
            collector_pid: nil

  @type t :: %__MODULE__{
    task: ExDag.DAG.DAGTask.t(),
    task_id: String.t(),
    handler: atom(),
    payload: map(),
    result: any(),
    error: any(),
    status: atom(),
    started_at: DateTime.t(),
    ended_at: DateTime.t(),
    collector_pid: pid()
  }

  def new(%DAGTask{}=task, payload, collector_pid) do
    struct!(__MODULE__,
      task: task,
      task_id: task.id,
      handler: task.handler,
      payload: payload,
      status: :running,
      started_at: DateTime.utc_now(),
      collector_pid: collector_pid
    )
  end
end
