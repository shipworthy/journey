defmodule Journey.Graph.ConfigTest do
  use ExUnit.Case, async: false

  setup do
    # Use start_owner! for better handling of spawned processes
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Journey.Repo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
    :ok
  end

  test "pre-configured graphs are loaded" do
    # Make sure the configuration is what we expect.
    # Changing graphs configuration might require corresponding changes in this test.
    assert Application.get_env(:journey, :graphs, []) |> Enum.count() == 2

    # Given the configuration, we should be able to fetch the graphs.
    assert nil != Journey.Graph.Catalog.fetch("test graph 1 Elixir.Journey.Test.Support", "1.0.0")
    assert nil != Journey.Graph.Catalog.fetch("test graph 2 Elixir.Journey.Test.Support", "1.0.0")
  end
end
