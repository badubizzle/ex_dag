defmodule ExDag.DAG.DAGTask do
  @moduledoc """
  A DAG Task
  """
  @derive {Jason.Encoder, except: [:__struct__, :last_run, :handler]}

  @derive {Inspect, except: [:__struct__, :last_run, :handler]}

  @enforce_keys [:id]
  defstruct id: nil,
            status: nil,
            last_run: nil,
            stop_on_failure: false,
            retries: 3,
            start_date: nil,
            data: nil,
            handler: nil

  require Logger

  @status_completed :completed
  @status_failed :failed
  @status_running :running

  @type t :: %__MODULE__{
          id: String.t(),
          status: atom(),
          last_run: ExDag.DAG.DAGTaskRun.t(),
          stop_on_failure: boolean(),
          retries: non_neg_integer(),
          start_date: DateTime.t(),
          data: map(),
          handler: atom() | nil
        }
  @doc """
  Create a new task
  """
  def new(opts) do
    Logger.debug("Creating new task: #{inspect(opts)}")
    id = Keyword.get(opts, :id)

    if is_binary(id) and byte_size(id) > 0 do
      struct(__MODULE__, opts)
    else
      {:error, :invalid_task_id}
    end
  end

  def set_handler(%__MODULE__{} = dag, handler) when is_atom(handler) do
    %__MODULE__{dag | handler: handler}
  end

  @doc """
  Validate a task
  """
  def validate(%__MODULE__{id: id, data: data, handler: handler})
      when is_binary(id) and byte_size(id) > 0 and is_atom(handler) and is_map(data) and
             not is_nil(handler) do
    true
  end

  def validate(%__MODULE__{}) do
    false
  end

  def is_pending(%__MODULE__{} = task) do
    task.status == nil
  end

  def is_completed(%__MODULE__{} = task) do
    task.status == status_completed()
  end

  def is_running(%__MODULE__{} = task) do
    task.status == status_running()
  end

  def status_completed() do
    @status_completed
  end

  def status_failed() do
    @status_failed
  end

  def status_running() do
    @status_running
  end
end
