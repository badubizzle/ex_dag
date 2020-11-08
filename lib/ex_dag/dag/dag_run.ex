defmodule ExDag.DAGRun do
  @moduledoc """
  Represents a running instance of a DAG
  """
  @enforce_keys [:id, :dag]
  defstruct id: nil,
            dag: nil,
            started_at: nil,
            ended_at: nil

  alias ExDag.DAG

  @type t :: %__MODULE__{
          id: String.t(),
          dag: DAG.t(),
          started_at: DateTime.t(),
          ended_at: DateTime.t()
        }

  @spec new(DAG.t()) :: t()
  def new(%DAG{} = dag) do
    id = generate_id()
    now = DateTime.utc_now()
    struct!(__MODULE__, dag: dag, id: id, started_at: now, ended_at: nil)
  end

  def generate_id() do
    random_string(10)
  end

  def random_string(length) do
    length
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64()
    |> binary_part(0, length)
  end
end
