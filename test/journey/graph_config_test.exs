defmodule Journey.GraphConfigTest do
  use ExUnit.Case, async: true

  test "pre-configured graphs are loaded" do
    # Make sure the configuration is what we expect.
    # Changing graphs configuration might require corresponding changes in this test.
    assert Application.get_env(:journey, :graphs, []) |> Enum.count() == 2

    # Given the configuration, we should be able to fetch the graphs.
    assert nil != Journey.Graph.Catalog.fetch!("test graph 1 Elixir.Journey.Test.Support")
    assert nil != Journey.Graph.Catalog.fetch!("test graph 2 Elixir.Journey.Test.Support")
  end
end
