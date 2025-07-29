defmodule Journey.Repo.Migrations.AddSweepPerformanceIndexes do
  use Ecto.Migration

  def change do
    create index(:computations, [:computation_type, :state, :execution_id],
             where:
               "computation_type IN ('schedule_once', 'schedule_recurring') AND state = 'not_set'",
             name: :computations_schedule_not_set_idx
           )

    create index(:computations, [:state, :deadline],
             where: "state = 'computing' AND deadline IS NOT NULL",
             name: :computations_computing_deadline_idx
           )

    create index(:values, [:execution_id, :node_name, :node_type, :set_time],
             where: "node_type IN ('schedule_once', 'schedule_recurring')",
             name: :values_schedule_lookup_idx
           )
  end
end
