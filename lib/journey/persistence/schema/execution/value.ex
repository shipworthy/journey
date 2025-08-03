defmodule Journey.Persistence.Schema.Execution.Value.JsonbScalar do
  @moduledoc false

  @behaviour Ecto.Type

  def type, do: :map

  def cast(val), do: {:ok, val}
  def load(val), do: {:ok, val}
  def dump(val), do: {:ok, val}

  def embed_as(_format), do: :self
  def equal?(term1, term2), do: term1 == term2
end

defmodule Journey.Persistence.Schema.Execution.Value do
  @moduledoc false

  use Journey.Schema.Base

  alias Journey.Persistence.Schema.Execution.ComputationType

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["VAL"]}}

  schema "values" do
    belongs_to(:execution, Journey.Persistence.Schema.Execution)
    field(:node_name, :string)
    field(:node_type, Ecto.Enum, values: [:input | ComputationType.values()])
    field(:node_value, Journey.Persistence.Schema.Execution.Value.JsonbScalar, default: nil)
    field(:set_time, :integer, default: nil)
    field(:ex_revision, :integer, default: nil)
    timestamps()
  end
end
