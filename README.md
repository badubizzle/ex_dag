# ExDag
Experimental implementation of Airflow DAGs in Elixir

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ex_dag` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_dag, "~> 0.1.0"}
  ]
end
```

----
ExDAG Task



Example

           a
          /\
         c  b
        /  \
       d    e
      / \  / \
     f   g h  i

### Structuring Tasks

Give the above we can say that
Task a depends on tasks c, and b
Task c depends on tasks d and e
and so on

```
a -> [b -> [], c -> [d -> [f, g], e-> [h, i]]]
```

We can only run Task a when task c and b are completed
We can only run task c when d and e are completed

### Running Tasks

We can run a task under 2 conditions

1. The task has no dependency or
2. All tasks it depends on are completed

Given this,
We can start running tasks *f, g, h, i, b* since they have no dependencies.

Taking advantage of Elixir processes we can run these tasks in concurrently

Once those tasks are completed we pick the next available tasks and run them.

### Task structure

Each task will have a *id, data, callback*
The callback is must be a function with arity 2

The callback can be either an anonymous function or a {m, f, _a}

The first argument to the callback is the task itself and the second argument is a map of the results from all child tasks it depends on

### Example Mathematic equation

```
2+3+4+5+6
```

We can represent this in a tree form as


          a(+)
          /  \
         c(+)  b(2)
        /   \
       d(+)   e(+)
      /   \    /   \
    f(6) g(5) i(4) h(3)


We can structure a tasks based on the node type
We have two types of nodes i.e value nodes and equation nodes.

For instance node a can have a task like this

```elixir
task_a = %{
    id: :a,
    data: %{op: :+},
    handker: handler_module
}

task_b = %{
    id: :b,
    data: %{value: 2},
    callback: handler_module
}
```

```elixir
defmodule MathHandler do
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
                IO.puts "Completed DAG run #{inspect(pid)} is down"
        end
    end
end
```
