defmodule Journey.Helpers.GrabBag do
  @moduledoc false

  def hash(""), do: ""
  def hash(nil), do: nil
  def hash(s) when is_binary(s), do: :crypto.hash(:md5, s) |> Base.encode64()

  def ids_of(c) when is_list(c), do: Enum.map(c, fn e -> e.id end)

  # Formats an integer with comma thousands separators (e.g. 1234567 -> "1,234,567", -1234 -> "-1,234").
  def delimit_integer(n) when is_integer(n) and n < 0, do: "-" <> delimit_integer(-n)

  def delimit_integer(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.reverse()
    |> String.graphemes()
    |> Enum.chunk_every(3)
    |> Enum.map_join(",", fn chunk -> Enum.join(chunk) end)
    |> String.reverse()
  end
end
