defmodule Journey.Repo.Migrations.AddSweepPerformanceIndexes do
  use Ecto.Migration

  def change do
    # Critical indexes for background sweeps

    # For ScheduleNodes.sweep - finding uncomputed schedule nodes
    create index(:computations, [:computation_type, :state, :execution_id],
             where:
               "computation_type IN ('schedule_once', 'schedule_recurring') AND state = 'not_set'",
             name: :computations_schedule_not_set_idx
           )

    # For Abandoned.sweep - finding computations past deadline
    create index(:computations, [:state, :deadline],
             where: "state = 'computing' AND deadline IS NOT NULL",
             name: :computations_computing_deadline_idx
           )

    # For UnblockedBySchedule.sweep - complex join optimization
    create index(:values, [:execution_id, :node_name, :node_type, :set_time],
             where: "node_type IN ('schedule_once', 'schedule_recurring')",
             name: :values_schedule_lookup_idx
           )

    # Index for execution_id already exists from initial migration
    # The [:execution_id, :state] index also already exists
  end
end
