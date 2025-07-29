defmodule Journey.Scheduler.SweepRunTest do
  use ExUnit.Case, async: true

  alias Journey.Scheduler.SweepRun

  describe "changeset/2" do
    test "valid changeset with required fields" do
      attrs = %{
        sweep_type: :schedule_nodes,
        started_at: System.os_time(:second)
      }

      changeset = SweepRun.changeset(%SweepRun{}, attrs)
      assert changeset.valid?
    end

    test "valid changeset with all fields" do
      now = System.os_time(:second)

      attrs = %{
        sweep_type: :schedule_nodes,
        started_at: now,
        completed_at: now + 5,
        executions_processed: 42
      }

      changeset = SweepRun.changeset(%SweepRun{}, attrs)
      assert changeset.valid?
    end

    test "invalid without sweep_type" do
      attrs = %{
        started_at: System.os_time(:second)
      }

      changeset = SweepRun.changeset(%SweepRun{}, attrs)
      refute changeset.valid?
      assert changeset.errors[:sweep_type] == {"can't be blank", [validation: :required]}
    end

    test "invalid without started_at" do
      attrs = %{
        sweep_type: :schedule_nodes
      }

      changeset = SweepRun.changeset(%SweepRun{}, attrs)
      refute changeset.valid?
      assert changeset.errors[:started_at] == {"can't be blank", [validation: :required]}
    end

    test "validates sweep_type inclusion" do
      attrs = %{
        sweep_type: :invalid_type,
        started_at: System.os_time(:second)
      }

      changeset = SweepRun.changeset(%SweepRun{}, attrs)
      refute changeset.valid?

      assert {"is invalid", _} = changeset.errors[:sweep_type]
    end

    test "accepts all valid sweep_types" do
      valid_types = [:schedule_nodes, :unblocked_by_schedule, :abandoned, :regenerate_schedule_recurring]

      for sweep_type <- valid_types do
        attrs = %{
          sweep_type: sweep_type,
          started_at: System.os_time(:second)
        }

        changeset = SweepRun.changeset(%SweepRun{}, attrs)
        assert changeset.valid?, "#{sweep_type} should be valid"
      end
    end
  end
end
