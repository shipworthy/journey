defmodule ReadmeTest do
  use ExUnit.Case, async: true
  import Journey.Node
  doctest_file("README.md")
end
