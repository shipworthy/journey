defmodule Journey.Repo.Migrations.AddGraphNameVersionIndex do
  use Ecto.Migration

  def change do
    # Composite index for graph_name and graph_version
    # This optimizes insights queries that filter by both fields
    create index(:executions, [:graph_name, :graph_version])
  end
end
