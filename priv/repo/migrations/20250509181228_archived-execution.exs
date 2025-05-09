defmodule :"Elixir.Journey.Repo.Migrations.Archived-execution" do
  use Ecto.Migration

  def change do
    alter table(:executions) do
      add :archived_at, :bigint, default: nil
    end
  end
end
