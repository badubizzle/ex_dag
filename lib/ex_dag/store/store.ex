defmodule ExDag.Store.Adapter do
  @moduledoc """
  Store Adapter Interface/Behaviour
  """
  @callback init(options :: Keyword.t()) :: {:ok, Keyword.t()} | {:error, any()}

  @callback save_dag(options :: Keyword.t(), dag :: ExDag.DAG.t()) :: :ok | {:error, atom()}
  @callback get_dag_path(options :: Keyword.t(), dag :: ExDag.DAG.t()) ::
              {:ok, binary()} | {:error, binary()}
  @callback save_dag_run(options :: Keyword.t(), dag_run :: ExDag.DAGRun.t()) ::
              :ok | {:error, atom()}
  @callback get_dags(Keyword.t()) :: map()

  @callback delete_dag(options :: Keyword.t(), dag :: ExDag.DAG.t()) :: :ok | {:error, term}

  @callback get_dag_runs(options :: Keyword.t(), dag :: ExDag.DAG.t()) :: map()
end

defmodule ExDag.Store do
  @moduledoc """
  Store Module
  """
  alias ExDag.DAG
  alias ExDag.DAGRun

  require Logger

  @spec init() :: :ok | {:error, any()}
  def init() do
    options = get_adapter_options()

    case get_adapter().init(options) do
      {:ok, _} -> :ok
      {:error, any} -> {:error, any}
    end
  end

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

  @spec completed?(DAGRun.t() | DAG.t()) :: boolean
  def completed?(%DAGRun{} = dag_run) do
    completed?(dag_run.dag)
  end

  def completed?(%DAG{} = dag) do
    DAG.completed?(dag)
  end

  def is_running(dag_run_id) do
    ExDag.DAG.DAGSupervisor.get_running_dags()
    |> Enum.map(fn %ExDag.DAGRun{id: run_id} ->
      run_id
    end)
    |> Enum.member?(dag_run_id)
  end

  def get_dags() do
    call(:get_dags)
  end

  def get_dag_runs() do
    []
  end

  def get_dag_runs(dag) do
    call(:get_dag_runs, dag)
  end

  def save_dag(%ExDag.DAG{} = dag) do
    call(:save_dag, dag)
  end

  def delete_dag(%ExDag.DAG{} = dag) do
    call(:delete_dag, dag)
  end

  def save_dag_run(dag_run) do
    Logger.debug("Saving dag run")
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
