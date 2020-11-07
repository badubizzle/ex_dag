defmodule ExDag.DAG.Utils.TaskHandler do
  @behaviour ExDag.DAG.Handlers.TaskHandler

  @impl true
  def run_task(task, payload) do
    wait = Enum.random(1000..2000)
    Process.sleep(wait)

    if rem(wait, 5) == 0 do
      Process.exit(self(), :kill)
    else
      case task.data do
        {:value, v} ->
          {:ok, v}

        {:op, :+} ->
          {:ok, Enum.reduce(payload, 0, fn {_k, v}, acc -> acc + v end)}

        _ ->
          IO.puts("Unhandled")
      end
    end
  end

  @impl true
  def on_success(_arg0, _arg1) do
  end
end
