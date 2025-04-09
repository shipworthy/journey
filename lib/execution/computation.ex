defmodule Journey.Execution.Computation do
  use Journey.Schema.Base
  alias Journey.Execution.ComputationState
  alias Journey.Execution.ComputationType

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["CMP"]}}

  schema "computations" do
    belongs_to(:execution, Journey.Execution)
    field(:node_name, :string)
    field(:computation_type, Ecto.Enum, values: ComputationType.values())
    field(:state, Ecto.Enum, values: ComputationState.values())
    field(:ex_revision_at_start, :integer, default: 0)
    field(:ex_revision_at_completion, :integer, default: 0)
    field(:scheduled_time, :integer, default: 0)
    field(:start_time, :integer)
    field(:completion_time, :integer)
    field(:deadline, :integer)
    field(:error_details, :string)
    timestamps()
  end
end
