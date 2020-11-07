defmodule ExDag.DAG.DAGSupervisor do
  use DynamicSupervisor

  require Logger

  alias ExDag.DAG

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    log = "Starting supervisor #{__MODULE__}"
    Logger.log(:info, log)
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def run_dag(%DAG{dag_id: dag_id}=dag) do
    log = "Calling #{__MODULE__}.run_dag/1 with args: #{inspect(dag_id)}"
    Logger.log(:info, log)

    spec = %{
      id: {ExDag.DAG.Server, dag_id},
      start: {ExDag.DAG.Server, :start_link, [dag]},
      restart: :temporary,
      type: :worker
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  def running_dags() do
    case Swarm.members(:dags) do
      pids when is_list(pids) ->

        Enum.map(pids , fn pid ->
            if Process.alive?(pid) do
              try do
                :sys.get_state(pid)
              rescue
                _ ->
                  nil
              end
            else
              nil
            end

        end)
        |> Enum.filter(&(!is_nil(&1)))
    end
  end

  def get_running_dags() do
    DynamicSupervisor.which_children(__MODULE__)
    |> Enum.map(fn {_, pid, _, _}->
      :sys.get_state(pid)
    end)
  end
end
