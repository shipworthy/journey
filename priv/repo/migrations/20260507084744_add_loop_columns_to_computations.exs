defmodule Journey.Repo.Migrations.AddLoopColumnsToComputations do
  use Ecto.Migration

  def change do
    alter table(:computations) do
      add :loop_state, :map, null: true
      add :loop_iteration, :integer, null: true
    end
  end
end
