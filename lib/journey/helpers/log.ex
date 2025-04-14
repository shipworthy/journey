defmodule Journey.Helpers.Log do
  @moduledoc false

  defmacro mf() do
    caller_function_tuple =
      case __CALLER__.function do
        nil -> {"<no caller>", 0}
        function -> function
      end

    "#{__CALLER__.module}.#{elem(caller_function_tuple, 0)}"
    |> String.trim_leading("Elixir.")
  end
end
