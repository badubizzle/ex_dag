defmodule MathHandler do
  @moduledoc false
  @behaviour ExDag.DAG.Handlers.TaskHandler

  @impl true
  def run_task(task, payload) do
    wait = Enum.random(5_000..20_000)
    Process.sleep(wait)

    if rem(wait, 5) == 0 do
      Process.exit(self(), :kill)
    else
      case task.data do
        %{value: v} ->
          {:ok, v}

        %{op: :+} ->
          {:ok, Enum.reduce(payload, 0, fn {_k, v}, acc -> acc + v end)}

        _ ->
          IO.puts("Unhandled")
      end
    end
  end

  @impl true
  def on_success(_arg0, _arg1) do
  end
end

defmodule MathDAG do
  @moduledoc false
  alias ExDag.DAG
  alias ExDag.DAG.Server
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun
  alias ExDag.DAG.Utils

  @behaviour ExDag.DAG.Handlers.DAGHandler

  require Logger

  def on_dag_completed(dag_run) do
    Utils.print_status(dag_run.dag)
    Utils.print_task_runs(dag_run.dag.task_runs)
  end

  def on_task_completed(_dag_run, task, result) do
    IO.puts("Completed task: #{inspect(task.id)} Result: #{inspect(result)}")
  end

  def on_task_started(_dag_run, task_run) do
    Logger.info("Started task #{task_run.task.id}")
  end

  def build_dag() do
    start_date = DateTime.utc_now() |> DateTime.add(5, :second)
    handler = MathHandler
    dag_id = "math"

    dag =
      DAG.new(dag_id)
      |> DAG.set_default_task_handler(handler)
      |> DAG.set_handler(__MODULE__)
      |> DAG.add_task!(id: "a", data: %{op: :+})
      |> DAG.add_task!(id: "b", data: %{value: 2}, parent: "a")
      |> DAG.add_task!(id: "c", data: %{op: :+}, parent: "a")
      |> DAG.add_task!(id: "d", data: %{op: :+}, parent: "c")
      |> DAG.add_task!(id: "e", data: %{op: :+}, parent: "c")
      |> DAG.add_task!(id: "f", data: %{value: 6}, parent: "d")
      |> DAG.add_task!(id: "g", data: %{value: 5}, start_date: start_date, parent: "d")
      |> DAG.add_task!(id: "h", data: %{value: 4}, parent: "e")
      |> DAG.add_task!(id: "i", data: %{value: 3}, parent: "e")

    dag
  end

  def start() do
    dag = build_dag()
    {:ok, pid} = Server.run_dag(dag)

    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, _, _, _} ->
        IO.puts("Completed DAG run #{inspect(pid)} is down")
    end
  end
end

defmodule ExDag.DAG.Utils do
  @moduledoc """
  Utility module for running and test DAGs
  """
  alias ExDag.DAG
  alias ExDag.DAG.Server
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun

  require Logger

  def on_task_completed(dag_run, _task, _result) do
    print_status(dag_run.dag)
    print_task_runs(dag_run.dag.task_runs)
  end

  def on_dag_completed(dag_run) do
    print_status(dag_run.dag)
    print_task_runs(dag_run.dag.task_runs)
  end

  defp get_task_headers() do
    [
      "Task ID",
      "Status",
      "Depends On",
      "Retries",
      "Start Date",
      "Started At",
      "Ended At",
      "Took",
      "Runs",
      "Result",
      "Payload"
    ]
  end

  def get_task_row_values(dag, {task_id, %DAGTask{last_run: nil} = task}) do
    deps =
      case Map.get(dag.task_deps, task_id, []) do
        [] -> "-"
        l -> Enum.join(l, ", ")
      end

    [task_id, :pending, deps, task.retries, task.start_date, "-", "-", "-", 0, "-", "-"]
  end

  def get_task_row_values(dag, {task_id, %DAGTask{last_run: %DAGTaskRun{} = last_run} = task}) do
    lapse =
      if !is_nil(last_run.ended_at) and !is_nil(last_run.started_at) do
        DateTime.diff(last_run.ended_at, last_run.started_at)
      else
        "-"
      end

    deps =
      case Map.get(dag.task_deps, task_id, []) do
        [] -> "-"
        l -> Enum.join(l, ", ")
      end

    [
      task_id,
      last_run.status,
      deps,
      task.retries,
      task.start_date,
      last_run.started_at || "-",
      last_run.ended_at || "-",
      lapse,
      DAG.get_runs(dag, task_id) |> Enum.count(),
      last_run.result || last_run.error,
      "#{inspect(last_run.payload)}"
    ]
  end

  def print_status(%DAG{} = dag) do
    header = get_task_headers()

    rows =
      dag.tasks
      |> Enum.map(fn task -> get_task_row_values(dag, task) end)

    if Enum.count(rows) > 0 do
      title = "Task status - #{dag.dag_id} (#{Enum.count(rows)})"
      s = TableRex.quick_render!(rows, header, title)
      IO.write("\r#{s}")
      IO.write("\n")
    else
      Logger.debug("No tasks found")
    end
  end

  def print_task_runs(task_runs) do
    header = [
      "Task ID",
      "Status",
      "Started At",
      "Ended At",
      "Took",
      "Result",
      "Payload"
    ]

    rows =
      task_runs
      |> Map.values()
      |> List.flatten()
      |> Enum.map(fn
        %DAGTaskRun{} = last_run ->
          lapse =
            if !is_nil(last_run.ended_at) and !is_nil(last_run.started_at) do
              DateTime.diff(last_run.ended_at, last_run.started_at)
            else
              "-"
            end

          [
            last_run.task_id,
            last_run.status,
            last_run.started_at || "-",
            last_run.ended_at || "-",
            lapse,
            last_run.result || last_run.error,
            "#{inspect(last_run.payload)}"
          ]
      end)

    if Enum.count(rows) > 0 do
      title = "Task Runs (#{Enum.count(rows)})"
      s = TableRex.quick_render!(rows, header, title)
      IO.write("\r#{s}")
      IO.write("\n")
    else
      Logger.debug("No tasks found")
    end
  end

  def test_eq() do
    dag_id = "equation"

    dag = build_dag(dag_id)

    {:ok, pid} = Server.run_dag(dag)
    pid
  end

  def build_dag(dag_id) do
    start_date = DateTime.utc_now() |> DateTime.add(5, :second)

    handler = ExDag.DAG.Utils.TaskHandler

    dag =
      DAG.new(dag_id)
      |> DAG.set_default_task_handler(handler)
      |> DAG.set_handler(__MODULE__)
      |> DAG.add_task!(id: "a", data: %{op: :+})
      |> DAG.add_task!(id: "b", data: %{value: 2}, parent: "a")
      |> DAG.add_task!(id: "c", data: %{op: :+}, parent: "a")
      |> DAG.add_task!(id: "d", data: %{op: :+}, parent: "c")
      |> DAG.add_task!(id: "e", data: %{op: :+}, parent: "c")
      |> DAG.add_task!(id: "f", data: %{value: 6}, parent: "d")
      |> DAG.add_task!(id: "g", data: %{value: 5}, start_date: start_date, parent: "d")
      |> DAG.add_task!(id: "h", data: %{value: 4}, parent: "e")
      |> DAG.add_task!(id: "i", data: %{value: 3}, parent: "e")

    dag
  end
end
