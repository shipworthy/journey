defmodule Journey.Scheduler.Introspection do
  @moduledoc false

  def execution_state(execution_id) do
    execution_id
    |> Journey.load()
    |> execution_state()
  end

  def readiness_state(
        ready?,
        conditions_met,
        conditions_not_met,
        computation_name
      ) do
    conditions_met =
      conditions_met
      |> compose_conditions_string("✅")

    conditions_not_met =
      conditions_not_met
      |> compose_conditions_string("🛑")

    icon = if(ready?, do: "✅", else: "🛑")

    """
    Node: #{inspect(computation_name)}

    Blocked?: #{inspect(!ready?)} #{icon}

    Upstream prerequisites:
    - met:
    #{conditions_met}

    - not met:
    #{conditions_not_met}
    """
  end

  defp compose_conditions_string([], _) do
    "    NONE"
  end

  defp compose_conditions_string(conditions, prefix) do
    conditions
    |> Enum.map_join("\n", fn %{upstream_node: v, f_condition: f_condition} ->
      fi = f_condition |> :erlang.fun_info()
      value = if v.set_time == nil, do: "<not set>", else: String.slice("#{inspect(v.node_value)}", 0..20)
      "    [#{prefix}] #{v.node_name}: &#{fi[:name]}/#{fi[:arity]} (rev: #{v.ex_revision}, val: #{inspect(value)})"
    end)
  end
end
