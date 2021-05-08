defmodule ExDag.Mix.Task.Utils do
  @moduledoc """
  Utility functions for generating dag modules
  """
  @app_name :ex_dag
  @templates_dir "templates"
  @dag_template "dag.html.eex"
  @task_handler_template "task_handler.html.eex"

  def gen_dag_module(dag_module, destination_dir, assigns \\ %{}) do
    assigns =
      Map.merge(
        assigns,
        %{
          module: Macro.camelize(dag_module)
        }
      )

    content = render_dag_module_template(assigns)
    module_file_path = Path.join([destination_dir, Macro.underscore(dag_module)])
    result = write_to_file("#{module_file_path}.ex", content)
    result
  end

  def gen_task_handler_module(dag_module, destination_dir, assigns \\ %{}) do
    assigns =
      Map.merge(
        assigns,
        %{
          module: "#{Macro.camelize(dag_module)}TaskHandler"
        }
      )

    content = render_template(template: @task_handler_template, data: assigns)
    module_file_path = Path.join([destination_dir, Macro.underscore(dag_module)])
    result = write_to_file("#{module_file_path}_task_handler.ex", content)
    result
  end

  def render_template_to_string(template_file_path, data) do
    result = EEx.eval_file(template_file_path, assigns: data)
    result
  end

  def render_template(template: template, data: data) do
    template_file_path = Path.join([:code.priv_dir(@app_name), @templates_dir, template])
    result = render_template_to_string(template_file_path, data)
    result
  end

  def render_dag_module_template(data) do
    result = render_template(template: @dag_template, data: data)
    result
  end

  def write_to_file(file_path, content) do
    File.write(file_path, content)
  end
end
