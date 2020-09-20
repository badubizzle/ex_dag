pid = ExDag.DAG.Utils.test_eq()

ref = Process.monitor(pid)

receive do
  {:DOWN, ^ref, _, _, _} ->
    IO.puts "Process #{inspect(pid)} is down"
end
