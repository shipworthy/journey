defmodule Journey.Execution do
  @moduledoc false

  use Journey.Schema.Base

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["EXEC"]}}

  schema "executions" do
    field(:graph_name, :string)
    field(:graph_version, :string)
    field(:archived_at, :integer, default: nil)
    has_many(:values, Journey.Execution.Value, preload_order: [desc: :ex_revision])
    has_many(:computations, Journey.Execution.Computation, preload_order: [desc: :ex_revision_at_completion])
    # has_many(:computations, Journey.Schema.Computation, preload_order: [asc: :ex_revision])
    field(:revision, :integer, default: 0)
    timestamps()
  end
end
