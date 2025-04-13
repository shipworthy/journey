defmodule Journey.Helpers.Random do
  @moduledoc false

  @digits "1234567890"
  @uppercase "ABDEGHJLMRTVXYZ"
  @lowercase String.downcase(@uppercase)

  def digits(), do: @digits
  def uppercase(), do: @uppercase
  def lowercase(), do: @lowercase
  def any_char_or_digit(), do: digits() <> uppercase() <> lowercase()

  def random_string(length \\ 12, dictionary \\ any_char_or_digit()) when is_number(length) and is_binary(dictionary) do
    Nanoid.generate(length, dictionary)
  end

  def object_id(prefix) when is_binary(prefix) do
    prefix <> random_string(20, digits() <> uppercase())
  end
end
