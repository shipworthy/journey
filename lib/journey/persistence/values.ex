defmodule Journey.Persistence.Values do
  @moduledoc false

  import Ecto.Query
  alias Journey.Schema.Execution.Value

  def load_from_db(execution_id, repo) do
    from(v in Value, where: v.execution_id == ^execution_id)
    |> repo.all()
    |> Enum.map(&convert_node_name_to_atom/1)
  end

  def convert_node_name_to_atom(%{node_name: node_name} = val) when is_struct(val, Value) and is_binary(node_name) do
    %Value{val | node_name: String.to_atom(node_name)}
  end
end
