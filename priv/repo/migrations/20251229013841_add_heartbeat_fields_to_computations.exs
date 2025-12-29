defmodule Journey.Repo.Migrations.AddHeartbeatFieldsToComputations do
  use Ecto.Migration

  def change do
    alter table(:computations) do
      add :last_heartbeat_at, :bigint, null: true
      add :heartbeat_deadline, :bigint, null: true
    end
  end
end
