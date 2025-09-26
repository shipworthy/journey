defmodule Journey.Persistence.Schema.Execution do
  @moduledoc false

  use Ecto.Schema
  @timestamps_opts [type: :integer, autogenerate: {System, :os_time, [:second]}]
  @foreign_key_type :string

  @primary_key {:id, :string, autogenerate: false}
  schema "executions" do
    field(:graph_name, :string)
    field(:graph_version, :string)
    field(:graph_hash, :string)
    field(:archived_at, :integer, default: nil)
    has_many(:values, Journey.Persistence.Schema.Execution.Value, preload_order: [desc: :ex_revision])

    has_many(:computations, Journey.Persistence.Schema.Execution.Computation,
      preload_order: [desc: :ex_revision_at_completion]
    )

    field(:revision, :integer, default: 0)
    timestamps()
  end
end
