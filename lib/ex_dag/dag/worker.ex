defmodule ExDag.DAG.Worker do
  @moduledoc """
  GenServer for running dag task
  """
  use GenServer

  require Logger

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

  def handle_continue(:run_task, %DAGTaskRun{handler: handler} = state) when handler != nil do
    result = apply(handler, :run_task, [state.task, state.payload])

    send(state.collector_pid, {:collect, self(), result})
    {:stop, :normal, state}
  end

  def handle_info({:done, result}, state) do
    {:stop, result, state}
  end
end
