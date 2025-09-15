defmodule Journey.Repo.Migrations.AddGinIndexToValuesNodeValue do
  use Ecto.Migration

  def change do
    create index(:values, [:node_value], using: :gin)
  end
end
