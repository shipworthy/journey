defmodule Journey.Repo.Migrations.OptimizeDatabaseIndexes do
  use Ecto.Migration

  def change do
    # Remove underutilized index
    drop(index(:computations, [:state, :deadline]))

    # Add optimized indexes for common query patterns
    create(index(:computations, [:computation_type, :state]))
    create(index(:values, [:node_name, :execution_id]))
    create(index(:values, [:set_time]))

    # Partial indexes for filtered queries
    create(index(:executions, [:id], where: "archived_at IS NULL", name: :executions_active_idx))

    create(
      index(:computations, [:scheduled_time],
        where: "scheduled_time IS NOT NULL",
        name: :computations_scheduled_idx
      )
    )
  end
end
