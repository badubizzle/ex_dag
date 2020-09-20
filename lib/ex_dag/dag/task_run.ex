defmodule ExDag.DAG.DAGTaskRun do
  @enforce_keys [:task_id, :task]
  defstruct task: nil,
            task_id: nil,
            callback: nil,
            payload: nil,
            result: nil,
            error: nil,
            status: nil,
            started_at: nil,
            ended_at: nil,
            collector_pid: nil

  def new(task, payload, collector_pid) do
    struct!(__MODULE__,
      task: task,
      task_id: task.id,
      callback: task.callback,
      payload: payload,
      status: :running,
      started_at: DateTime.utc_now(),
      collector_pid: collector_pid
    )
  end
end
