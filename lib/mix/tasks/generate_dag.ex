defmodule Mix.Tasks.Exdag.Create.Dag do
  @moduledoc """
  Generate a dag module
  """
  use Mix.Task

  @shortdoc "Generate a DAG"

  alias ExDag.Mix.Task.Utils

  @impl Mix.Task
  def run(args) do
    # template_file_path = Path.join([:code.priv_dir(:ex_dag), "templates", "dag.html.eex"])
    dag_module = hd(args)
    # assigns = %{
    #   dag_module: Macro.camelize(dag_module)
    # }
    # content = EEx.eval_file(template_file_path, assigns: assigns)
    dags_path = Application.get_env(:ex_dag, :generator_dir)
    File.mkdir_p!(dags_path)
    # module_file_path = Path.join([dags_path, Macro.underscore(dag_module)])
    Utils.gen_dag_module(dag_module, dags_path)
    Utils.gen_task_handler_module(dag_module, dags_path)
  end
end
