defmodule Journey.Node.ConditionsTest do
  use ExUnit.Case
  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  describe "provided?" do
    test "returns true if the node has a value" do
      graph =
        Journey.new_graph(
          "umbrella forecast graph, doctest for false? #{Journey.Helpers.Random.random_string()}",
          "v1.0.0",
          [
            input(:it_will_rain_tomorrow),
            compute(
              :prepare_umbrella,
              unblocked_when(:it_will_rain_tomorrow, &true?/1),
              fn %{it_will_rain_tomorrow: true} -> {:ok, "prepare my umbrella!"} end
            ),
            compute(
              :prepare_bike,
              unblocked_when(:it_will_rain_tomorrow, &false?/1),
              fn %{it_will_rain_tomorrow: false} -> {:ok, "prepare my bike!"} end
            )
          ]
        )

      execution = Journey.start_execution(graph)
      execution = Journey.set_value(execution, :it_will_rain_tomorrow, false)
      assert {:ok, "prepare my bike!"} = Journey.get_value(execution, :prepare_bike, wait_any: true)
      assert Journey.values(execution) |> redact() == %{it_will_rain_tomorrow: false, prepare_bike: "prepare my bike!"}

      execution = Journey.set_value(execution, :it_will_rain_tomorrow, true)
      assert {:ok, "prepare my umbrella!"} = Journey.get_value(execution, :prepare_umbrella, wait_any: true)

      assert Journey.values(execution) |> redact() == %{
               it_will_rain_tomorrow: true,
               prepare_umbrella: "prepare my umbrella!"
             }
    end
  end

  defp redact(m), do: Map.drop(m, [:execution_id, :last_updated_at])
end
