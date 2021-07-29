defmodule Journey.Utilities do
  def curent_unix_time_ms() do
    DateTime.utc_now() |> DateTime.to_unix(:millisecond)
  end

  def convert_key_strings_to_existing_atoms!(string_keyed_map) do
    string_keyed_map
    |> Map.new(fn {k, v} ->
      {String.to_existing_atom(k), v}
    end)
  end
end
