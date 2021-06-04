defmodule ExDag.Tracer do
  @moduledoc """
  Trace calls to functions in a module
  """
  use GenServer
  require Logger

  def trace(modules) do
    Logger.info("[Tracer] Started tracing #{inspect(modules)}")
    GenServer.start(__MODULE__, modules)
  end

  @impl GenServer
  def init(modules) do
    :erlang.trace(:all, true, [:call])

    for module <- modules do
      :erlang.trace_pattern({module, :_, :_}, [{:_, [], [{:return_trace}]}])
    end

    {:ok, nil}
  end

  @impl GenServer
  def handle_info({:trace, _, :call, {module, function, arguments}}, state) do
    trace_call(module, function, arguments)
    {:noreply, state}
  end

  def handle_info({:trace, _, :return_from, {module, function, arguments}, result}, state) do
    trace_return_value(module, function, arguments, result)
    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  defp trace_call(module, function, arguments) do
    args_string =
      arguments
      |> Enum.map(&inspect/1)
      |> Enum.join(",")

    IO.puts("[Tracer] Calling #{module}.#{function}(#{args_string})")
  end

  defp trace_return_value(module, function, arguments, result) do
    trace_call(module, function, arguments)
    IO.puts("[Tracer] Returned #{inspect(result)}")
  end
end
