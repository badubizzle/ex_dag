defmodule ExDag.DAG.DAGTask do
  @enforce_keys [:id, :callback]
  defstruct id: nil,
            callback: nil,
            status: nil,
            last_run: nil,
            stop_on_failure: false,
            retries: 3,
            start_date: nil,
            data: nil,
            on_success: nil,
            on_failure: nil,
            on_retry: nil

  @doc """
  Create a new task
  """
  def new(opts) do
    struct!(__MODULE__, opts)
  end

  @doc """
  Validate a task
  """
  def validate(%__MODULE__{id: id, callback: callback})
      when not is_nil(id) and is_function(callback, 2) do
    true
  end

  def validate(%__MODULE__{id: id, callback: {m, f, a}})
      when not is_nil(id) and is_atom(m) and is_atom(f) and is_list(a) do
    true
  end

  def validate(%__MODULE__{id: id, callback: {m, f}})
      when not is_nil(id) and is_atom(m) and is_atom(f) do
    true
  end

  def validate(_t) do
    false
  end
end
