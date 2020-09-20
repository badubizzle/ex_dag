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

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ex_dag](https://hexdocs.pm/ex_dag).


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
    data: {:op, :+},
    callback: fn _t, %{b: b , c: c} ->
    b + c
    end
}

task_b = %{
    id: :b,
    data: {:value, 2},
    callback: fn %{data: {:value, v}}, _ ->
        v
    end
}
```

```elixir

    alias ExDag.DAG.Server
    alias ExDag.DAG
    alias ExDag.DAG.DAGTask
    alias ExDag.DAG.DAGTaskRun

callback = fn task, payload ->
    wait = Enum.random(1000..2000)
    Process.sleep(wait)

    if rem(wait, 5) == 0 do
        # simulate failed task
        Process.exit(self(), :kill)
    else
        case task.data do
        {:value, v} ->
            {:ok, v}

        {:op, :+} ->
            {:ok, Enum.reduce(payload, 0, fn {_k, v}, acc -> acc + v end)}

        _ ->
            :ok
        end
    end
    end

    start_date = DateTime.utc_now() |> DateTime.add(5, :second)

    dag =
    DAG.new("my dag")
    |> DAG.add_task!(id: :a, callback: callback, data: {:op, :+})
    |> DAG.add_task!(id: :b, callback: callback, data: {:value, 2})
    |> DAG.add_task!(id: :c, callback: callback, data: {:op, :+})
    |> DAG.add_task!(id: :d, callback: callback, data: {:op, :+})
    |> DAG.add_task!(id: :e, callback: callback, data: {:op, :+})
    |> DAG.add_task!(id: :f, callback: callback, data: {:value, 6})
    |> DAG.add_task!(id: :g, callback: callback, data: {:value, 5}, start_date: start_date)
    |> DAG.add_task!(id: :h, callback: callback, data: {:value, 4})
    |> DAG.add_task!(id: :i, callback: callback, data: {:value, 3})
    |> DAG.add_dependency(:a, :b)
    |> DAG.add_dependency(:a, :c)
    |> DAG.add_dependency(:c, :d)
    |> DAG.add_dependency(:c, :e)
    |> DAG.add_dependency(:d, :f)
    |> DAG.add_dependency(:d, :g)
    |> DAG.add_dependency(:e, :h)
    |> DAG.add_dependency(:e, :i)


    ExDag.DAG.Utils.start_dag_registry()
    {:ok, pid} = Server.run_dag(dag)

    ref = Process.monitor(pid)

    receive do
    {:DOWN, ^ref, _, _, _} ->
        IO.puts "Process #{inspect(pid)} is down"
    end



```


