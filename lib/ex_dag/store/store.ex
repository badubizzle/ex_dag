defmodule ExDag.Store.Adapter do
  @moduledoc """
  Store Adapter Interface/Behaviour
  """
  @callback save_dag(options :: Keyword.t(), dag :: ExDag.DAG.t()) :: :ok | {:error, atom()}
  @callback get_dag_path(options :: Keyword.t(), dag :: ExDag.DAG.t()) ::
              {:ok, binary()} | {:error, binary()}
  @callback save_dag_run(options :: Keyword.t(), dag_run :: ExDag.DAGRun.t()) ::
              :ok | {:error, atom()}
  @callback get_dags(Keyword.t()) :: map()
end

defmodule ExDag.Store do
  @moduledoc """
  Store Module
  """
  @spec get_adapter :: atom()
  def get_adapter() do
    Application.get_env(:ex_dag, :store_adapter, ExDag.Store.FileStore)
  end

  @spec get_adapter_options :: Keyword.t()
  def get_adapter_options() do
    Application.get_env(:ex_dag, :store_adapter_options, [])
  end

  @spec get_dag_path(ExDag.DAG.t()) :: {:ok, binary()} | {:error, binary()}
  def get_dag_path(dag) do
    call(:get_dag_path, dag)
  end

  def get_dags() do
    call(:get_dags)
  end

  def get_dag_runs() do
  end

  def get_dag_runs(dag) do
    call(:get_dag_runs, dag)
  end

  def save_dag(%ExDag.DAG{} = dag) do
    call(:save_dag, dag)
  end

  def save_dag_run(dag_run) do
    call(:save_dag_run, dag_run)
  end

  @spec call(f :: atom()) :: any()
  defp call(f) when is_atom(f) do
    adapter = get_adapter()
    options = get_adapter_options()
    apply(adapter, f, [options])
  end

  @spec call(f :: atom(), args :: any()) :: any()
  defp call(f, args) when is_atom(f) do
    adapter = get_adapter()
    options = Keyword.new(get_adapter_options())
    apply(adapter, f, [options, args])
  end
end
