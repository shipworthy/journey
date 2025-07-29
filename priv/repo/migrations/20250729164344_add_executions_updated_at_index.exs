defmodule Journey.Repo.Migrations.AddExecutionsUpdatedAtIndex do
  use Ecto.Migration

  def change do
    # Index for the schedule_nodes sweep optimization
    # Used in the query: e.updated_at >= ^cutoff_time AND is_nil(e.archived_at)
    # Partial index only includes active (non-archived) executions
    create index(:executions, [:updated_at],
             where: "archived_at IS NULL",
             name: :executions_updated_at_active_idx
           )
  end
end
