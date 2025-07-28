defmodule Journey.Repo.Migrations.AddMissingPerformanceIndexes do
  use Ecto.Migration

  def change do
    # Additional index for computation queries that filter by state alone
    # This helps background sweeps that look for computations in specific states
    create index(:computations, [:state])

    # Index for execution queries filtering by graph_name and archived status
    # This helps queries that list executions by graph and filter active ones
    create index(:executions, [:graph_name, :archived_at])
  end
end
