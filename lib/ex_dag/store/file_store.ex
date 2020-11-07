defmodule ExDag.Store.FileStore do
  @moduledoc """
  DAGs store implementation using file system.
  """
  @behaviour ExDag.Store.Adapter

  @impl true
  def save_dag(options, dag) do
    dags_path = get_dags_path(options)
    file_name = "dag_file_#{dag.dag_id}"
    path = Path.join(dags_path, file_name)
    File.write(path, :erlang.term_to_binary(dag), [:write])
  end

  @impl true
  def save_dag_run(options, %{dag_id: dag_id}=dag) do
    dags_path = get_dags_path(options)
    runs_paths = Path.join([dags_path, "runs", dag_id])

  end

  @impl true
  def get_dags(options) when is_list(options) do
    dags_path = Keyword.get(options, :dags_path)
    IO.inspect(dags_path)

    case File.dir?(dags_path) do
      true ->
        File.ls!(dags_path)
        |> Enum.map(fn path ->
          dag =
            File.read!(Path.join([dags_path, path]))
            |> :erlang.binary_to_term()
            {dag.dag_id, dag}
        end)
        |> Map.new()
    end
  end

  def get_dag_runs(dag_path, dag_id) do
    runs_path = Path.join([dag_path, dag_id, "runs"])
  end

  defp get_dags_path(options) do
    Keyword.get(options, :dags_path)
  end
end
