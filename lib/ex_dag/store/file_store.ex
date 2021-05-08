defmodule ExDag.Store.FileStore do
  @moduledoc """
  DAGs store implementation using file system.
  """
  @behaviour ExDag.Store.Adapter
  alias ExDag.DAG
  require Logger

  @impl true
  def init_store(options) do
    dags_path = get_dags_path(options)
    File.mkdir_p(dags_path)
    {:ok, options}
  end

  @impl true
  def save_dag(options, dag) do
    dags_path = get_dags_path(options)
    file_name = "dag_file_#{dag.dag_id}"
    path = Path.join(dags_path, file_name)
    Logger.debug("Saving dag to file: #{path}")
    File.write(path, :erlang.term_to_binary(dag), [:write])
  end

  @impl true
  def get_dag_path(options, dag) do
    dags_path = get_dags_path(options)
    file_name = "dag_file_#{dag.dag_id}"
    {:ok, Path.join([dags_path, file_name])}
  end

  @impl true
  def save_dag_run(options, dag_run) do
    dags_path = get_dags_path(options)
    dag = dag_run.dag
    runs_path = Path.join([dags_path, "runs", dag.dag_id])
    File.mkdir_p(runs_path)
    path = Path.join([runs_path, dag_run.id])
    Logger.debug("Saving dag run to file: #{path}")
    File.write(path, :erlang.term_to_binary(dag_run), [:write])
  end

  @impl true
  def get_dags(options) do
    dags_path = Keyword.get(options, :dags_path)

    Logger.debug("Getting dags from path: #{dags_path}")

    case File.dir?(dags_path) do
      true ->
        File.ls!(dags_path)
        |> Enum.filter(fn path ->
          !File.dir?(Path.join([dags_path, path]))
        end)
        |> Enum.map(fn path ->
          dag =
            File.read!(Path.join([dags_path, path]))
            |> :erlang.binary_to_term()

          {dag.dag_id, dag}
        end)
        |> Map.new()
    end
  end

  @impl true
  @spec get_dag_runs(options :: Keyword.t(), dag :: DAG.t()) :: map()
  def get_dag_runs(options, %DAG{} = dag) do
    dags_path = get_dags_path(options)
    runs_path = Path.join([dags_path, "runs", dag.dag_id])

    if File.exists?(runs_path) and File.dir?(runs_path) do
      File.ls!(runs_path)
      |> Enum.map(fn path ->
        dag_run =
          File.read!(Path.join([runs_path, path]))
          |> :erlang.binary_to_term()

        {dag_run.id, dag_run}
      end)
      |> Map.new()
    else
      %{}
    end
  end

  @impl true
  @spec delete_dag(options :: Keyword.t(), dag :: DAG.t()) :: :ok
  def delete_dag(options, dag) do
    {:ok, dag_file} = get_dag_path(options, dag)

    runs_path = Path.join([get_dags_path(options), "runs", dag.dag_id])
    File.rm_rf(runs_path)
    File.rm!(dag_file)
  end

  @impl true
  @doc """
  Returns a DAGRun for the given dag_id and run_id
  """
  def get_dag_run(options, dag_id, run_id) do
    dags_path = get_dags_path(options)
    runs_path = Path.join([dags_path, "runs", dag_id])
    run_file_path = Path.join([runs_path, run_id])

    Logger.debug("Getting dag run from file: #{run_file_path}")

    case File.read(run_file_path) do
      {:ok, content} ->
        %ExDag.DAGRun{} = dag_run = :erlang.binary_to_term(content)
        {:ok, dag_run}

      {:error, _} ->
        {:error, :not_found}
    end
  end

  defp get_dags_path(options) do
    Keyword.get(options, :dags_path)
  end
end
