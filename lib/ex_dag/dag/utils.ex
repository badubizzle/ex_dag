defmodule ExDag.DAG.Utils do
  alias ExDag.DAG.Server
  alias ExDag.DAG
  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun

  require Logger

  def on_task_completed(dag, _task, _result) do
    print_status(dag)
    print_task_runs(dag.task_runs)
  end

  defp print_status(%DAG{} = dag) do

    header = [
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

    rows =
      Enum.map(dag.tasks, fn
        {task_id, %DAGTask{last_run: nil} = task} ->
          deps =
            case Map.get(dag.task_deps, task_id, []) do
              [] -> "-"
              l -> Enum.join(l, ", ")
            end

          [task_id, :pending, deps, task.retries, task.start_date, "-", "-", "-", 0, "-", "-"]

        {task_id, %DAGTask{last_run: %DAGTaskRun{} = last_run} = task} ->
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
      end)

    if Enum.count(rows) > 0 do
      title = "Task status - #{dag.dag_id} (#{Enum.count(rows)})"
      s = TableRex.quick_render!(rows, header, title)
      IO.write("\r#{s}")
      IO.write("\n")
    else
      Logger.debug("No tasks found")
    end
  end

  defp print_task_runs(task_runs) do

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
    callback = fn task, payload ->
      wait = Enum.random(1000..2000)
      Process.sleep(wait)

      if rem(wait, 5) == 0 do
        Process.exit(self(), :kill)
      else
        case task.data do
          {:value, v} ->
            {:ok, v}

          {:op, :+} ->
            {:ok, Enum.reduce(payload, 0, fn {_k, v}, acc -> acc + v end)}

          _ ->
            IO.puts("Unhandled")
        end
      end
    end

    start_date = DateTime.utc_now() |> DateTime.add(5, :second)

    dag =
      DAG.new(dag_id)
      |> DAG.add_task!(id: :a, callback: callback, data: {:op, :+})
      |> DAG.add_task!(id: :b, callback: callback, data: {:value, 2}, parent: :a)
      |> DAG.add_task!(id: :c, callback: callback, data: {:op, :+}, parent: :a)
      |> DAG.add_task!(id: :d, callback: callback, data: {:op, :+}, parent: :c)
      |> DAG.add_task!(id: :e, callback: callback, data: {:op, :+}, parent: :c)
      |> DAG.add_task!(id: :f, callback: callback, data: {:value, 6}, parent: :d)
      |> DAG.add_task!(id: :g, callback: callback, data: {:value, 5}, start_date: start_date, parent: :d)
      |> DAG.add_task!(id: :h, callback: callback, data: {:value, 4}, parent: :e)
      |> DAG.add_task!(id: :i, callback: callback, data: {:value, 3}, parent: :e)

    dag
  end
end
