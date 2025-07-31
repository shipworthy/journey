defmodule Journey.Test.Support.Helpers do
  import WaitForIt
  import ExUnit.Assertions

  def wait_for_value(execution, node_name, expected_value, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 20_000)
    frequency = Keyword.get(opts, :frequency, 500)

    assert wait(
             (fn ->
                {:ok, expected_value} == Journey.get_value(execution, node_name)
              end).(),
             timeout: timeout,
             frequency: frequency
           )
  end

  def wait_for_new_value(execution, node_name, old_value, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 20_000)
    frequency = Keyword.get(opts, :frequency, 500)

    assert wait(
             (fn ->
                case Journey.get_value(execution, node_name) do
                  {:ok, value} when value != old_value -> true
                  _ -> false
                end
              end).(),
             timeout: timeout,
             frequency: frequency
           )
  end
end
