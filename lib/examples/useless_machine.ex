defmodule UselessMachine do
  import Journey.Node

  def graph() do
    Journey.new_graph(
      "useless application",
      "v1.0.0",
      [
        input(:switch),
        mutate(:paw, [:switch], &lol_no/1, mutates: :switch)
      ]
    )
  end

  def lol_no(%{switch: switch}) do
    IO.puts("paw says: '#{switch}? lol no'")
    {:ok, "off"}
  end
end
