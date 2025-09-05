defmodule Journey.Persistence.Schema.SweepRun do
  @moduledoc false

  use Journey.Persistence.Schema.Base
  import Ecto.Changeset

  @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["SWEEP"]}}

  schema "sweep_runs" do
    field(:sweep_type, Ecto.Enum,
      values: [
        :schedule_nodes,
        :unblocked_by_schedule,
        :abandoned,
        :regenerate_schedule_recurring,
        :missed_schedules_catchall,
        :stalled_executions
      ]
    )

    field(:started_at, :integer)
    field(:completed_at, :integer)
    field(:executions_processed, :integer)

    timestamps()
  end

  def changeset(sweep_run, attrs) do
    sweep_run
    |> cast(attrs, [:sweep_type, :started_at, :completed_at, :executions_processed])
    |> validate_required([:sweep_type, :started_at])
  end
end
