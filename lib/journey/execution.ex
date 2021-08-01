defmodule Journey.ExecutionDbRecord do
  use Ecto.Schema

  @primary_key {:id, :string, []}
  @timestamps_opts [type: :integer, autogenerate: {Journey.Utilities, :curent_unix_time_ms, []}]
  schema "execution" do
    field(:execution_data, :map)
    timestamps()
  end

  def convert_to_execution_struct!(execution_db_record) do
    execution =
      execution_db_record
      |> Journey.Utilities.convert_key_strings_to_existing_atoms!()

    transformed_values =
      execution[:values]
      |> Journey.Utilities.convert_key_strings_to_existing_atoms!()
      |> Map.new(fn {k, v} ->
        {k, Journey.Value.convert_from_string_keys(v)}
      end)

    execution
    |> Map.put(:values, transformed_values)
  end
end
