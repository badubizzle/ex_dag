defmodule ExDag.Store.Adapter do

  @callback save_dag(Keyword.t, ExDag.DAG.t) :: :ok | {:error, atom()}
  @callback save_dag_run(Keyword.t, ExDag.DAGRun.t) :: :ok | {:error, atom()}
  @callback get_dags(Keyword.t) :: map()
end

defmodule ExDag.Store do

  def get_adapter() do
    Application.get_env(:ex_dag, :store_adapter, ExDag.Store.FileStore)
  end

  def get_adapter_options() do
    Application.get_env(:ex_dag, :store_adapter_options, [])
  end

  def get_dags() do
    call(:get_dags)
  end

  def save_dag(dag) do
    call(:save_dag, dag)
  end

  def save_dag_run(dag) do
    call(:save_dag_run, dag)
  end

  defp call(f) when is_atom(f) do
    adapter = get_adapter()
    options = get_adapter_options()
    apply(adapter, f, [options])
  end

  defp call(f, opts) when is_atom(f) do
    adapter = get_adapter()
    options = get_adapter_options()
    apply(adapter, f, [options | opts])
  end
end
