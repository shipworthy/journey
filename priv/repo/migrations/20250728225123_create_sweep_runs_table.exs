defmodule Journey.Repo.Migrations.CreateSweepRunsTable do
  use Ecto.Migration

  def change do
    create table(:sweep_runs, primary_key: false) do
      add(:id, :string, primary_key: true)
      add(:sweep_type, :string, null: false)
      add(:started_at, :bigint, null: false)
      add(:completed_at, :bigint, null: true)
      add(:executions_processed, :integer, null: true)

      timestamps(type: :bigint)
    end

    create index(:sweep_runs, [:sweep_type, :completed_at])
    create index(:sweep_runs, [:sweep_type, :started_at])
  end
end
