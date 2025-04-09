defmodule Journey.Execution.Value do
  use Journey.Schema.Base

  alias Journey.Execution.ComputationType

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["VAL"]}}

  schema "values" do
    belongs_to(:execution, Journey.Execution)
    field(:node_name, :string)
    field(:node_type, Ecto.Enum, values: [:input | ComputationType.values()])
    field(:node_value, :map)
    field(:set_time, :integer, default: nil)
    field(:ex_revision, :integer, default: 0)
    timestamps()
  end
end
