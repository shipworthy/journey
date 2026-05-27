defmodule Journey.Helpers.GrabBagTest do
  use ExUnit.Case, async: true
  import Journey.Helpers.GrabBag

  describe "delimit_integer" do
    test "small numbers are unchanged" do
      assert delimit_integer(0) == "0"
      assert delimit_integer(5) == "5"
      assert delimit_integer(999) == "999"
    end

    test "inserts comma thousands separators" do
      assert delimit_integer(1_000) == "1,000"
      assert delimit_integer(1_234_567) == "1,234,567"
      assert delimit_integer(1_000_000) == "1,000,000"
    end

    test "handles negative numbers" do
      assert delimit_integer(-1_234) == "-1,234"
      assert delimit_integer(-999) == "-999"
    end
  end
end
