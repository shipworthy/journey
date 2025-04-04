defmodule Journey.Helpers.RandomTest do
  use ExUnit.Case, async: true
  import Journey.Helpers.Random

  describe "object_id" do
    test "basic" do
      oid = object_id("OID")
      assert String.length(oid) == 23
      assert String.starts_with?(oid, "OID")
    end
  end

  describe "random_string" do
    @digits "1234567890" |> String.graphemes() |> MapSet.new()
    test "length" do
      assert String.length(random_string(15)) == 15
    end

    test "defaults" do
      assert String.length(random_string()) == 12
    end

    test "digits" do
      r = random_string(12, digits()) |> String.graphemes()
      assert length(r) == 12
      assert MapSet.difference(MapSet.new(r), @digits) == MapSet.new([])
    end

    test "uppercase" do
      r = random_string(12, uppercase())
      assert String.length(r) == 12
      assert String.upcase(r) == r
      assert MapSet.difference(@digits, MapSet.new(String.graphemes(r))) == @digits
    end

    test "lowercase" do
      r = random_string(12, lowercase())
      assert String.length(r) == 12
      assert String.downcase(r) == r
      assert MapSet.difference(@digits, MapSet.new(String.graphemes(r))) == @digits
    end
  end
end
