defmodule Journey.Node.UpstreamDependencies.DocTest do
  use ExUnit.Case, async: false

  setup do
    # Use start_owner! for better handling of spawned processes in doctests
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Journey.Repo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
    :ok
  end

  doctest Journey.Node.UpstreamDependencies
end
