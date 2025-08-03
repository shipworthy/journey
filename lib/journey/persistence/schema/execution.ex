defmodule Journey.Persistence.Schema.Execution do
  @moduledoc false

  use Journey.Persistence.Schema.Base

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["EXEC"]}}

  schema "executions" do
    field(:graph_name, :string)
    field(:graph_version, :string)
    field(:archived_at, :integer, default: nil)
    has_many(:values, Journey.Persistence.Schema.Execution.Value, preload_order: [desc: :ex_revision])

    has_many(:computations, Journey.Persistence.Schema.Execution.Computation,
      preload_order: [desc: :ex_revision_at_completion]
    )

    field(:revision, :integer, default: 0)
    timestamps()
  end
end
