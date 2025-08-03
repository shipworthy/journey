defmodule Journey.Execution.Computation do
  @moduledoc false

  use Journey.Schema.Base
  alias Journey.Execution.ComputationState
  alias Journey.Execution.ComputationType

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["CMP"]}}

  schema "computations" do
    belongs_to(:execution, Journey.Execution)
    field(:node_name, :string)
    field(:computation_type, Ecto.Enum, values: ComputationType.values())
    field(:state, Ecto.Enum, values: ComputationState.values())
    field(:ex_revision_at_start, :integer, default: nil)
    field(:ex_revision_at_completion, :integer, default: nil)
    field(:scheduled_time, :integer, default: nil)
    field(:start_time, :integer, default: nil)
    field(:completion_time, :integer, default: nil)
    field(:deadline, :integer, default: nil)
    field(:error_details, :string, default: nil)
    # revisions of the upstream nodes used to compute this value – e.g. %{upstream_node1: 6, upstream_node2: 9}
    field(:computed_with, :map, default: nil)
    timestamps()
  end
end
