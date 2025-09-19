defmodule UselessMachine do
  import Journey.Node

  @moduledoc """
  This module ([lib/examples/useless_machines.ex](https://github.com/markmark206/journey/blob/main/lib/examples/useless_machine.ex)) contains an example of building a Useless Machine using Journey.

  Here is an example of running the useless Machine:

  ```elixir
  iex> graph = UselessMachine.graph()
  iex> execution = Journey.start_execution(graph)
  iex> Journey.get_value(execution, :switch)
  {:error, :not_set}
  iex> Journey.get_value(execution, :paw)
  {:error, :not_set}
  iex> Journey.set(execution, :switch, "on")
  iex> # updating switch triggers :paw
  iex> Journey.get_value(execution, :paw, wait: :any)
  {:ok, "updated :switch"}
  iex> # :paw set switch back to "off"
  iex> Journey.get_value(execution, :switch, wait: :any)
  {:ok, "off"}
  ```
  """

  @doc """
  This function defines the graph for the Useless Machine.
  It starts with a switch input and mutates the state to "off" when the switch
  is toggled, simulating the behavior of a Useless Machine.
  """
  def graph() do
    Journey.new_graph(
      "useless machine example graph",
      "v1.0.0",
      [
        input(:switch),
        mutate(:paw, [:switch], &lol_no/1, mutates: :switch)
      ]
    )
  end

  @doc """
  This function simulates the paw's response when the switch is toggled.
  It prints a message and mutates the state of the :switch node to "off".
  """
  def lol_no(%{switch: switch}) do
    IO.puts("paw says: '#{switch}? lol no'")
    {:ok, "off"}
  end
end
