defmodule Journey.Repo.Migrations.ComputationIndices do
  use Ecto.Migration

  def change do
    create index(:computations, [:execution_id, :node_name, :state])
  end
end
