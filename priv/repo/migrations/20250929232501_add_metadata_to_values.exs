defmodule Journey.Repo.Migrations.AddMetadataToValues do
  use Ecto.Migration

  def change do
    alter table(:values) do
      add(:metadata, :jsonb)
    end
  end
end
