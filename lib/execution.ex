defmodule Journey.Execution do
  use Journey.Schema.Base

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["EXEC"]}}

  schema "executions" do
    field(:graph_name, :string)
    field(:graph_version, :string)
    has_many(:values, Journey.Execution.Value, preload_order: [desc: :node_name])
    has_many(:computations, Journey.Execution.Computation, preload_order: [desc: :node_name])
    # has_many(:computations, Journey.Schema.Computation, preload_order: [asc: :ex_revision])
    field(:revision, :integer, default: 0)
    timestamps()
  end
end
