defmodule Journey.Repo.Migrations.AddGraphHashToExecutions do
  use Ecto.Migration

  def change do
    alter table(:executions) do
      add(:graph_hash, :string)
    end
  end
end
