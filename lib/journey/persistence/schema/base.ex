defmodule Journey.Persistence.Schema.Base do
  @moduledoc false
  defmacro __using__(_) do
    quote do
      use Ecto.Schema

      @primary_key {:id, :string, autogenerate: {Journey.Helpers.Random, :object_id, ["OID"]}}
      @timestamps_opts [type: :integer, autogenerate: {System, :os_time, [:second]}]
      @foreign_key_type :string
    end
  end
end
