defmodule Journey.Execution.Persistence do
  # alias Journey.Execution

  # def create_new(graph_name, inputs_and_steps) do
  #   %Execution{
  #     graph_name: graph_name,
  #     revision: 0
  #   }
  #   |> Journey.Repo.insert()

  #   _node_inserts =
  #     Enum.map(inputs_and_steps, fn
  #       %Journey.Graph.Input{name: name} = input_node ->
  #         IO.inspect(input_node, label: :input_node)

  #       # %Journey.Execution.Node{
  #       #   name: name,
  #       #   type: :input,
  #       #   value: nil
  #       # }

  #       %Journey.Graph.Step{name: name, upstream_nodes: upstream_nodes} = step_node ->
  #         IO.inspect(step_node, label: :step_node)
  #         # %Journey.Schema.Computation{
  #         #   name: name,
  #         #   type: :step,
  #         #   value: nil
  #         # }
  #     end)

  #   # |> Repo.insert()
  # end

  # def set_value(execution, node_name, value) do
  # end
end
