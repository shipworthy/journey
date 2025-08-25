defmodule Journey.Repo.Migrations.ChangeComputationsErrorDetailsToText do
  use Ecto.Migration

  def change do
    alter table(:computations) do
      modify(:error_details, :text, default: nil)
    end
  end
end
