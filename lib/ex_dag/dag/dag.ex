defmodule ExDag.DAG do
  @moduledoc """
  Represents a DAG dag
  """

  alias ExDag.DAG.DAGTask
  alias ExDag.DAG.DAGTaskRun
  require Logger

  @derive {Inspect, only: [:dag_id, :status]}
  @derive {Jason.Encoder, only: [:dag_id, :status]}

  @status_init :init

  @enforce_keys [:dag_id, :g]
  defstruct dag_id: nil,
            status: :init,
            g: nil,
            completed: nil,
            running: nil,
            failed: nil,
            timer: nil,
            tasks: nil,
            task_runs: nil,
            task_deps: nil,
            handler: nil,
            task_handler: nil

  @status_running :running
  @status_done :done
  @status_init :init
  @root :__root

  @type t :: %__MODULE__{
          dag_id: String.t(),
          status: atom(),
          g: Graph.t(),
          completed: map(),
          running: map(),
          failed: map(),
          tasks: map(),
          task_runs: map(),
          task_deps: map(),
          handler: atom() | nil
        }

  @doc """
  Create a new DAG dag struct
  """
  def new(dag_id) when is_binary(dag_id) and byte_size(dag_id) > 0 do
    new(dag_id, nil, nil)
  end

  def new(_dag_id) do
    {:error, :invalid_dag_id}
  end

  def new(dag_id, handler, task_handler)
      when is_binary(dag_id) and byte_size(dag_id) > 0 and is_atom(handler) do
    g = Graph.new(type: :directed)
    # |> Graph.add_vertex(@root)

    # root_task = DAGTask.new(id: @root, handler: :none)
    running = %{}
    failed = %{}
    completed = %{}
    tasks = %{}
    task_runs = %{}
    task_deps = %{}

    struct!(__MODULE__,
      dag_id: dag_id,
      g: g,
      failed: failed,
      running: running,
      completed: completed,
      tasks: tasks,
      task_runs: task_runs,
      task_deps: task_deps,
      handler: handler,
      status: :init,
      task_handler: task_handler
    )
  end

  def get_tasks(%__MODULE__{} = dag) do
    Map.keys(dag.tasks) -- [@root]
  end

  @spec set_handler(ExDag.DAG.t(), atom) :: ExDag.DAG.t()
  def set_handler(%__MODULE__{} = dag, handler) when is_atom(handler) do
    %__MODULE__{dag | handler: handler}
  end

  def set_default_task_handler(%__MODULE__{} = dag, handler) when is_atom(handler) do
    %__MODULE__{dag | task_handler: handler}
  end

  def set_tasks_handler(%__MODULE__{} = dag, handler) when is_atom(handler) do
    tasks =
      Enum.map(dag.tasks, fn {key, %DAGTask{} = t} ->
        {key, %DAGTask{t | handler: handler}}
      end)
      |> Map.new()

    %__MODULE__{dag | tasks: tasks}
  end

  @doc """
  Returns true or false if DAG  has valid DAG structure
  """
  def validate_for_run(%__MODULE__{g: g}) do
    Graph.is_tree?(g)
  end

  def validate_for_run(_dag) do
    false
  end

  @spec get_task(ExDag.DAG.t(), any) :: any
  def get_task(%__MODULE__{} = dag, task_id) do
    Map.get(dag.tasks, task_id)
  end

  @spec add_task(ExDag.DAG.t(), keyword | ExDag.DAG.DAGTask.t()) ::
          {:error, :invalid_task | :no_parent_task | :task_exists} | {:ok, ExDag.DAG.t()}
  def add_task(dag, task_or_opts) do
    do_add_task(dag, task_or_opts)
  end

  @spec add_task!(ExDag.DAG.t(), keyword | ExDag.DAG.DAGTask.t()) :: ExDag.DAG.t()
  def add_task!(dag, task_or_opts) do
    case do_add_task(dag, task_or_opts) do
      {:ok, %__MODULE__{} = new_dag} ->
        new_dag

      error ->
        throw(error)
    end
  end

  @spec add_task!(ExDag.DAG.t(), ExDag.DAG.DAGTask.t(), any) :: ExDag.DAG.t()
  def add_task!(dag, task_or_opts, parent_task_id) do
    case add_task(dag, task_or_opts, parent_task_id) do
      {:ok, %__MODULE__{} = new_dag} ->
        new_dag

      error ->
        throw(error)
    end
  end

  @spec add_task(ExDag.DAG.t(), ExDag.DAG.DAGTask.t(), any) ::
          {:error, :invalid_task | :no_parent_task | :task_exists} | {:ok, ExDag.DAG.t()}
  def add_task(
        %__MODULE__{status: @status_init, task_handler: default_handler} = dag,
        %DAGTask{handler: handler} = task,
        parent_task_id
      ) do
    case get_task(dag, parent_task_id) do
      %DAGTask{} = parent_task ->
        task =
          if is_nil(handler) do
            %DAGTask{task | handler: default_handler}
          else
            task
          end

        add_task_with_parent(dag, task, parent_task)

      _ ->
        {:error, :no_parent_task}
    end
  end

  defp do_add_task(%__MODULE__{status: @status_init, task_handler: task_handler} = dag, opts)
       when is_list(opts) do
    parent = Keyword.get(opts, :parent, nil)

    opts =
      case Keyword.get(opts, :handler, nil) do
        nil ->
          Keyword.merge(opts, handler: task_handler)

        _handler ->
          opts
      end

    if is_nil(parent) do
      task = DAGTask.new(opts)

      if DAGTask.validate(task) do
        do_add_task(dag, task)
      else
        {:error, :invalid_dag_task}
      end
    else
      opts = Keyword.delete(opts, :parent)
      task = DAGTask.new(opts)

      if DAGTask.validate(task) do
        add_task(dag, task, parent)
      else
        {:error, :invalid_dag_task}
      end
    end
  end

  defp do_add_task(
         %__MODULE__{task_handler: default_handler} = dag,
         %DAGTask{handler: handler} = task
       )
       when is_atom(default_handler) or is_atom(handler) do
    task =
      if is_nil(handler) do
        %DAGTask{task | handler: default_handler}
      else
        task
      end

    Logger.info("Adding new task: #{inspect(Map.from_struct(task))}")

    case DAGTask.validate(task) do
      true ->
        case Map.has_key?(dag.tasks, task.id) do
          false ->
            tasks = Map.put(dag.tasks, task.id, task)
            dag = %__MODULE__{dag | tasks: tasks}
            {:ok, update_graph(dag, Graph.add_vertex(dag.g, task.id, {:info, task}))}

          _ ->
            {:error, :task_exists}
        end

      _ ->
        {:error, :invalid_task}
    end
  end

  defp add_task_with_parent(dag, task, parent_task) do
    case DAGTask.validate(task) do
      true ->
        case do_add_task(dag, task) do
          {:ok, dag} ->
            add_dependency(dag, parent_task.id, task.id)

          error ->
            error
        end

      _any ->
        {:error, :invalid_task}
    end
  end

  defp add_dependency(%__MODULE__{status: @status_init} = dag, %DAGTask{id: task1_id}, %DAGTask{
         id: task2_id
       }) do
    add_dependency(dag, task1_id, task2_id)
  end

  defp add_dependency(%__MODULE__{status: @status_init} = dag, task1_id, task2_id) do
    # add edge and update label with deps
    if Map.has_key?(dag.tasks, task1_id) and Map.has_key?(dag.tasks, task2_id) do
      edge = Graph.Edge.new(task1_id, task2_id)

      updated_g =
        dag.g
        |> Graph.add_edge(edge)
        |> Graph.label_vertex(task1_id, {:deps, task2_id})

      dag = update_graph(dag, updated_g)
      {:ok, %__MODULE__{dag | task_deps: build_task_deps(dag)}}
    else
      {:error, :invalid_task}
    end
  end

  @doc """
  Returns a list of tasks that the given task depends on
  """
  @spec get_deps(ExDag.DAG.t(), task_id :: any) :: list()
  def get_deps(%__MODULE__{} = dag, task_id) do
    Map.get(dag.task_deps, task_id, [])
  end

  @spec get_deps_map(ExDag.DAG.t()) :: map()
  def get_deps_map(%__MODULE__{} = dag) do
    dag.task_deps
  end

  @doc """
  Returns all the runs for a DAG task
  """
  @spec get_runs(ExDag.DAG.t(), task_id :: any) :: list()
  def get_runs(%__MODULE__{} = dag, task_id) do
    dag.task_runs
    |> Map.get(task_id, [])
  end

  defp update_graph(dag, g) do
    %__MODULE__{dag | g: g}
  end

  defp build_task_deps(%__MODULE__{} = dag) do
    dag.g
    |> Graph.edges()
    |> Enum.group_by(fn %Graph.Edge{v1: task1_id} ->
      task1_id
    end)
    |> Enum.map(fn {task_id, deps} ->
      {task_id, Enum.map(deps, & &1.v2)}
    end)
    |> Map.new()
  end

  def status_running() do
    @status_running
  end

  def status_done() do
    @status_done
  end

  def status_init() do
    @status_init
  end

  @doc """
  Returns True if the last task (or tasks) in the DAG is completed
  """
  @spec completed?(ExDag.DAG.t()) :: boolean
  def completed?(%__MODULE__{} = dag) do
    tasks = get_last_tasks(dag)

    Enum.all?(tasks, fn
      {_task_id, %DAGTask{} = task} ->
        DAGTask.is_completed(task)

      task_id ->
        DAGTask.is_completed(get_task(dag, task_id))
    end)
  end

  @spec get_last_tasks(ExDag.DAG.t()) :: list
  def get_last_tasks(%__MODULE__{} = dag) do
    for v <- Graph.vertices(dag.g), Graph.in_degree(dag.g, v) == 0 do
      v
    end
  end

  def sorted_tasks(%__MODULE__{tasks: tasks} = dag) do
    task_ids = Map.keys(tasks)
    # Deps: %{a: [:b, :c], c: [:d, :e], d: [:f, :g], e: [:h, :i]}
    # Sorted Ids: [:a, :c, :e, :i, :h, :d, :g, :f, :b]

    # Deps: %{a: [:b, :c], c: [:d, :e], d: [:f, :g], e: [:h, :i]}
    # Sorted Ids: [:a, :b, :c, :d, :f, :g, :e, :h, :i]

    g =
      Graph.new(type: :directed)
      |> Graph.add_vertices(task_ids)

    edges =
      Enum.map(dag.task_deps, fn {task_id, deps} ->
        Enum.map(deps, fn dep -> {task_id, dep} end)
      end)
      |> List.flatten()

    g = Graph.add_edges(g, edges)
    sorted_ids = Graph.postorder(g)

    sorted_ids
    |> Enum.reverse()
    |> Enum.map(fn task_id ->
      {task_id, Map.from_struct(get_task(dag, task_id))}
    end)
    |> Map.new()
  end

  def get_completed_tasks(%__MODULE__{} = dag) do
    Enum.filter(dag.tasks, fn task ->
      DAGTask.is_completed(task)
    end)
  end

  def get_pending_tasks(%__MODULE__{} = dag) do
    Enum.filter(dag.tasks, fn task ->
      DAGTask.is_pending(task)
    end)
  end

  def get_running_tasks(%__MODULE__{} = dag) do
    Enum.filter(dag.tasks, fn task ->
      DAGTask.is_running(task)
    end)
  end

  @doc """
  Clear failed taks. This is necessary for resuming DAGs
  """
  @spec clear_failed_tasks_runs(ExDag.DAG.t()) :: ExDag.DAG.t()
  def clear_failed_tasks_runs(%__MODULE__{tasks: tasks} = dag) do
    failed_ids =
      tasks
      |> Map.keys()
      |> Enum.filter(fn t_id ->
        !should_run_task(dag, t_id)
      end)

    Logger.info("Failed tasks: #{inspect(failed_ids)}")

    tasks =
      Enum.reduce(tasks, tasks, fn {task_id, task}, t ->
        if Enum.member?(failed_ids, task_id) do
          Map.put(t, task_id, %DAGTask{task | last_run: nil})
        else
          t
        end
      end)

    %__MODULE__{dag | tasks: tasks}
  end

  def should_run_task(%__MODULE__{} = dag, task_id) do
    %DAGTask{last_run: last_run} = task = Map.get(dag.tasks, task_id)

    failed = DAGTask.status_failed()

    case last_run do
      %DAGTaskRun{status: ^failed} ->
        if task.stop_on_failure do
          false
        else
          task_runs = Map.get(dag.task_runs, task_id)
          task.retries && Enum.count(task_runs) < task.retries
        end

      _ ->
        true
    end
  end

  defimpl String.Chars, for: __MODULE__ do
    def to_string(dag) do
      "#DAG{tasks: #{inspect(dag.tasks)}}"
    end
  end
end
