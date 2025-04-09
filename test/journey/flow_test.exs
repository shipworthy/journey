defmodule Journey.FlowTest do
  use ExUnit.Case

  import Journey

  describe "flow" do
    test "sunny day" do
      execution =
        create_graph()
        |> Journey.start_execution()

      # |> IO.inspect(label: :execution_original)

      #        |> Journey.set_value(:birth_day, 26)
      #        |> IO.inspect(label: :execution_updated)
      #        |> Journey.set_value(:birth_month, "April")
      #        |> Journey.set_value(:first_name, "Mario")
      #        |> Journey.wait(:library_of_congress_record)

      #      assert Journey.values(execution) == %{
      #               first_name: {:set, "Mario"},
      #               birth_day: {:set, 26},
      #               birth_month: {:set, "April"},
      #               astrological_sign: {:set, "Taurus"},
      #               horoscope: {:set, "🍪s await, Taurus Mario!"},
      #               library_of_congress_record: {:failed, "lol no, Mario."}
      #             }
    end
  end

  defp create_graph() do
    Journey.new_graph(
      "horoscope workflow, success #{__MODULE__}",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        step(:astrological_sign, [:birth_month, :birth_day], fn %{birth_month: _birth_month, birth_day: _birth_day} ->
          Process.sleep(1000)
          {:ok, "Taurus"}
        end),
        step(:horoscope, [:first_name, :astrological_sign], fn %{first_name: name, astrological_sign: sign} ->
          Process.sleep(1000)
          {:ok, "🍪s await, #{sign} #{name}!"}
        end),
        step(:library_of_congress_record, [:horoscope, :first_name], fn %{horoscope: _horoscope, first_name: first_name} ->
          Process.sleep(1000)
          {:error, "lol no, #{first_name}."}
        end)
      ]
    )
  end
end
