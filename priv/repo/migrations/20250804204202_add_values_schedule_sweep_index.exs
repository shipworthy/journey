defmodule Journey.Repo.Migrations.AddValuesScheduleSweepIndex do
  use Ecto.Migration

  def change do
    # Index to optimize the unblocked_by_schedule sweep query
    # This query filters on node_value <= now (or as bigint) and set_time >= cutoff
    # for schedule_once and schedule_recurring nodes
    create index(:values, [:execution_id, :node_name, :node_type, :node_value, :set_time],
             where:
               "node_type IN ('schedule_once', 'schedule_recurring') AND set_time IS NOT NULL",
             name: :values_schedule_sweep_idx
           )
  end
end
