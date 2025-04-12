defmodule Journey.Repo.Migrations.AddExecutionsAndNodes do
  use Ecto.Migration

  def change do
    create table(:executions, primary_key: false) do
      add(:id, :string, primary_key: true)
      add(:graph_name, :string)
      add(:graph_version, :string)
      add(:revision, :bigint, default: 0)
      timestamps(type: :bigint)
    end

    create(index(:executions, [:graph_name]))

    create table(:values, primary_key: false) do
      add(:id, :string, primary_key: true)
      add(:execution_id, :string)
      add(:node_name, :string)
      add(:node_type, :string)
      add(:node_value, :jsonb)
      add(:set_time, :bigint, default: nil)
      add(:ex_revision, :bigint, default: nil)
      timestamps(type: :bigint)
    end

    create(index(:values, [:execution_id]))
    create(index(:values, [:execution_id, :node_name]))

    create table(:computations, primary_key: false) do
      add(:id, :string, primary_key: true)
      add(:execution_id, :string)
      add(:node_name, :string)
      add(:computation_type, :string)
      add(:state, :string)
      add(:ex_revision_at_start, :bigint, default: nil)
      add(:ex_revision_at_completion, :bigint, default: nil)
      add(:scheduled_time, :bigint, default: nil)
      add(:start_time, :bigint, default: nil)
      add(:completion_time, :bigint, default: nil)
      add(:deadline, :bigint, default: nil)
      add(:error_details, :string, default: nil)
      timestamps(type: :bigint)
    end

    create(index(:computations, [:execution_id]))
    create(index(:computations, [:state, :deadline]))
    create(index(:computations, [:execution_id, :state]))
  end
end
