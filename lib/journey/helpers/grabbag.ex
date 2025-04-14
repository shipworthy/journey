defmodule Journey.Helpers.GrabBag do
  @moduledoc false

  def hash(""), do: ""
  def hash(nil), do: nil
  def hash(s) when is_binary(s), do: :crypto.hash(:md5, s) |> Base.encode64()

  def ids_of(c) when is_list(c), do: Enum.map(c, fn e -> e.id end)
end
