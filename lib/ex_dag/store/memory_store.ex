defmodule ExDag.Store.MemoryStoreData do
  @moduledoc """
  Backend data module for ExDag.Store.MemoryStore
  """
  @enforce_keys [:dags, :runs, :options]
  defstruct dags: %{},
            runs: %{},
            options: %{}

  alias ExDag.DAG
  alias ExDag.DAGRun

  def new(options) do
    struct!(__MODULE__, runs: %{}, dags: %{}, options: options)
  end

  def add_dag(%__MODULE__{dags: dags} = data, %DAG{} = dag) do
    dags = Map.put(dags, dag.dag_id, dag)
    %__MODULE__{data | dags: dags}
  end

  def get_run(%__MODULE__{runs: runs}, dag_id, run_id) do
    run =
      runs
      |> Map.get(dag_id, %{})
      |> Map.get(run_id)

    run
  end

  def get_runs(%__MODULE__{runs: runs}, dag_id) do
    runs
    |> Map.get(dag_id, %{})
    |> Enum.map(fn {run_id, run} ->
      {run_id, run}
    end)
  end

  def get_dags(%__MODULE__{dags: dags}) do
    dags
  end

  def add_run(%__MODULE__{runs: runs} = data, %DAGRun{dag: %DAG{} = dag} = dag_run) do
    dag_id = dag.dag_id

    dag_runs =
      runs
      |> Map.get(dag_id, %{})
      |> Map.put(dag_run.id, dag_run)

    runs = Map.put(runs, dag_id, dag_runs)

    %__MODULE__{data | runs: runs}
  end

  def delete_dag(%__MODULE__{dags: dags} = data, dag_id) do
    dags = Map.delete(dags, dag_id)
    %__MODULE__{data | dags: dags}
  end
end

defmodule ExDag.Store.MemoryStore do
  @moduledoc """
  In-memory implementation of ExDag.Store.Adapter

  Uses a GenServer to maintain DAGs and DAGRuns
  """

  use GenServer
  @behaviour ExDag.Store.Adapter

  alias ExDag.Store.MemoryStoreData
  alias ExDag.DAG
  alias ExDag.DAGRun

  require Logger

  # GenServer stuff
  @impl GenServer
  def init(%MemoryStoreData{} = state) do
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:get_dags, _from, state) do
    dags = MemoryStoreData.get_dags(state)
    {:reply, dags, state}
  end

  def handle_call({:get_dag_runs, %DAG{} = dag, _options}, _from, %MemoryStoreData{} = state) do
    runs = MemoryStoreData.get_runs(state, dag.dag_id)

    Logger.info("Get data runs => #{inspect(runs)}")
    {:reply, runs, state}
  end

  def handle_call({:get_dag_run, dag_id, run_id, _options}, _from, %MemoryStoreData{} = state) do
    {:reply, {:ok, MemoryStoreData.get_run(state, dag_id, run_id)}, state}
  end

  def handle_call(
        {:save_dag_run, %DAGRun{} = dag_run, _options},
        _from,
        %MemoryStoreData{} = state
      ) do
    Logger.debug("Saving dag run :#{dag_run.id}")
    state = MemoryStoreData.add_run(state, dag_run)
    {:reply, {:ok, []}, state}
  end

  def handle_call({:save_dag, %DAG{} = dag, _options}, _from, %MemoryStoreData{} = state) do
    Logger.debug("Saving dag :#{dag.dag_id}")
    state = MemoryStoreData.add_dag(state, dag)
    {:reply, {:ok, []}, state}
  end

  def handle_call({:delete_dag, %DAG{} = dag, _options}, _from, %MemoryStoreData{} = state) do
    Logger.debug("Saving dag :#{dag.dag_id}")
    state = MemoryStoreData.delete_dag(state, dag)
    {:reply, {:ok, []}, state}
  end

  # Store stuff
  @impl ExDag.Store.Adapter
  def init_store(options) do
    state = MemoryStoreData.new(options)
    GenServer.start(__MODULE__, state, name: __MODULE__)
  end

  @impl ExDag.Store.Adapter
  def get_dags(_options) do
    GenServer.call(__MODULE__, :get_dags)
  end

  @impl ExDag.Store.Adapter
  def save_dag(options, dag) do
    GenServer.call(__MODULE__, {:save_dag, dag, options})
  end

  @impl ExDag.Store.Adapter
  def delete_dag(options, dag) do
    GenServer.call(__MODULE__, {:delete_dag, dag, options})
  end

  @impl ExDag.Store.Adapter
  def save_dag_run(options, dag_run) do
    GenServer.call(__MODULE__, {:save_dag_run, dag_run, options})
  end

  @impl ExDag.Store.Adapter
  def get_dag_runs(options, dag) do
    GenServer.call(__MODULE__, {:get_dag_runs, dag, options})
  end

  @impl ExDag.Store.Adapter
  def get_dag_path(_options, _dag) do
    {:error, "unsupported"}
  end

  @impl ExDag.Store.Adapter
  def get_dag_run(options, dag_id, run_id) do
    GenServer.call(__MODULE__, {:get_dag_run, dag_id, run_id, options})
  end
end
