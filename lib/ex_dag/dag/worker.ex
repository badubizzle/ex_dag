defmodule ExDag.DAG.Worker do
  use GenServer

  alias ExDag.DAG.DAGTaskRun

  def run_task(%DAGTaskRun{} = task) do
    GenServer.start_link(__MODULE__, task, [])
  end

  def start_link(task) do
    GenServer.start_link(__MODULE__, task, [])
  end

  def init(%DAGTaskRun{} = task) do
    {:ok, task, {:continue, :run_task}}
  end

  def handle_continue(:run_task, %DAGTaskRun{callback: callback} = state) when callback != nil do
    result =
      case callback do
        {m, f} ->
          apply(m, f, [state.task, state.payload])

        {m, f, _a} ->
          apply(m, f, [state.task, state.payload])

        f when is_function(f, 2) ->
          f.(state.task, state.payload)
      end

    send(state.collector_pid, {:collect, self(), result})
    {:stop, :normal, state}
  end

  def handle_info({:done, result}, state) do
    {:stop, result, state}
  end
end
