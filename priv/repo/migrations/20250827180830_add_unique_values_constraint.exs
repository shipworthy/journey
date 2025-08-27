defmodule Journey.Repo.Migrations.AddUniqueValuesConstraint do
  use Ecto.Migration

  def change do
    # Add unique index on (execution_id, node_name) to enforce data integrity
    # and optimize JOIN performance for value filtering queries
    create unique_index(:values, [:execution_id, :node_name],
             name: :values_execution_node_unique_idx
           )
  end
end
