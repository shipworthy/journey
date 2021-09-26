defmodule Journey.Utilities do
  def curent_unix_time_ms() do
    DateTime.utc_now() |> DateTime.to_unix(:millisecond)
  end

  def to_atom(an_atom) when is_atom(an_atom) do
    an_atom
  end

  def to_atom(an_atom_as_string) when is_binary(an_atom_as_string) do
    String.to_atom(an_atom_as_string)
  end

  def to_existing_atom(an_atom) when is_atom(an_atom) do
    an_atom
  end

  def to_existing_atom(an_atom_as_string) when is_binary(an_atom_as_string) do
    String.to_existing_atom(an_atom_as_string)
  end

  def convert_key_strings_to_existing_atoms!(string_keyed_map) do
    string_keyed_map
    |> Map.new(fn {k, v} ->
      {String.to_existing_atom(k), v}
    end)
  end
end
