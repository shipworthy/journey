defmodule Journey.Repo.Migrations.CreateExecutions do
  use Ecto.Migration

  def change do
    create table(:execution, primary_key: false) do
      add(:id, :string, primary_key: true)
      add(:execution_data, :map)
      timestamps(type: :bigint)
    end
  end
end
