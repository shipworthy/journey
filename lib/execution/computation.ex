defmodule Journey.Execution.Computation do
  use Journey.Schema.Base
  import Journey.Execution.ComputationState, only: [values: 0]
  alias Journey.Execution.ComputationType
  # @computation_types [
  #   :unknown,
  #   :compute,
  #   :mutation,
  #   :pulse_once,
  #   :pulse_recurring
  # ]

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["CMP"]}}

  schema "computations" do
    # field(:execution_id, :string)
    belongs_to(:execution, Journey.Execution)
    field(:node_name, :string)
    field(:computation_type, Ecto.Enum, values: ComputationType.values())
    field(:state, Ecto.Enum, values: values())
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
