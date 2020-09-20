defmodule ExDag.DAG do
  @moduledoc """
  Represents a DAG dag
  """

  alias ExDag.DAG.DAGTask

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
            on_task_completed: nil

  @doc """
  Create a new DAG dag struct
  """
  def new(dag_id) do
    new(dag_id, {ExDag.DAG.Utils, :on_task_completed, []})
  end
  def new(dag_id, on_task_completed) when is_binary(dag_id) do
    g = Graph.new(type: :directed)
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
      on_task_completed: on_task_completed,
      status: :init
    )
  end

  @doc """
  Returns true or false if DAG  has valid DAG structure
  """
  def validate_for_run(%__MODULE__{g: g}) do
    Enum.count(Graph.vertices(g)) <= 1 or Graph.is_tree?(g)
  end

  def validate_for_run(_dag) do
    false
  end

  def get_task(%__MODULE__{} = dag, task_id) do
    Map.get(dag.tasks, task_id)
  end

  def add_task(dag, task_or_opts) do
    do_add_task(dag, task_or_opts)
  end

  def add_task!(dag, task_or_opts) do
    case do_add_task(dag, task_or_opts) do
      {:ok, %__MODULE__{} = new_dag} ->
        new_dag
      error ->
        throw(error)
    end
  end

  def add_task!(dag, task_or_opts, parent_task_id) do
    case add_task(dag, task_or_opts, parent_task_id) do
      {:ok, %__MODULE__{} = new_dag} ->
        new_dag
      error ->
        throw(error)
    end
  end

  def add_task(%__MODULE__{status: @status_init} = dag, %DAGTask{} = task, parent_task_id) do
    case get_task(dag, parent_task_id) do
      %DAGTask{} = parent_task ->
        case DAGTask.validate(task) do
          true ->
            case do_add_task(dag, task) do
              {:ok, dag} ->
                  {:ok, add_dependency(dag, parent_task.id, task.id)}
              error ->
                error
            end
          any ->
            IO.inspect([any])
            {:error, :invalid_task}
        end
      _ ->
        {:error, :no_parent_task}
    end
  end

  defp do_add_task(%__MODULE__{status: @status_init} = dag, opts) when is_list(opts) do

    parent = Keyword.get(opts, :parent, nil)
    if is_nil(parent) do
      task = DAGTask.new(opts)
      do_add_task(dag, task)
    else
      opts = Keyword.delete(opts, :parent)
      task = DAGTask.new(opts)
      add_task(dag, task, parent)
    end
  end

  defp do_add_task(%__MODULE__{}=dag, %DAGTask{}=task) do
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


  def add_dependency(%__MODULE__{status: @status_init} = dag, %DAGTask{id: task1_id}, %DAGTask{
        id: task2_id
      }) do
    add_dependency(dag, task1_id, task2_id)
  end

  def add_dependency(%__MODULE__{status: @status_init} = dag, task1_id, task2_id) do
    # add edge and update label with deps
    if Map.has_key?(dag.tasks, task1_id) and Map.has_key?(dag.tasks, task2_id) do
      edge = Graph.Edge.new(task1_id, task2_id)

      updated_g =
        dag.g
        |> Graph.add_edge(edge)
        |> Graph.label_vertex(task1_id, {:deps, task2_id})
      dag = update_graph(dag, updated_g)
      %__MODULE__{dag | task_deps: build_task_deps(dag)}
    else
      {:error, :invalid_task}
    end
  end

  def get_deps(%__MODULE__{} = dag, task_id) do
    Map.get(dag.task_deps, task_id, [])
  end

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

  defimpl String.Chars, for: __MODULE__ do
    def to_string(dag) do
      "#DAG{tasks: #{inspect(dag.tasks)}}"
    end
  end

  defimpl Inspect, for: __MODULE__ do
    def inspect(dag, _opts) do
      to_string(dag)
    end
  end
end
