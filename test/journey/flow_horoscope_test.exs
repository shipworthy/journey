defmodule Journey.FlowTest do
  use ExUnit.Case, async: true

  import Journey

  describe "flow" do
    test "sunny day" do
      execution =
        create_graph()
        |> Journey.start_execution()
        |> Journey.set_value(:birth_day, 26)
        |> Journey.set_value(:birth_month, "April")
        |> Journey.set_value(:first_name, "Mario")

      assert Journey.values(execution) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: {:set, "Mario"},
               horoscope: :not_set,
               library_of_congress_record: :not_set
             }

      assert Journey.get_value(execution, :astrological_sign) == {:error, :not_set}
      assert Journey.get_value(execution, :horoscope) == {:error, :not_set}
      assert Journey.get_value(execution, :library_of_congress_record) == {:error, :not_set}
      assert Journey.get_value(execution, :birth_day) == {:ok, 26}

      Process.sleep(4_000)

      assert Journey.get_value(execution, :astrological_sign) == {:ok, "Taurus"}
      assert Journey.get_value(execution, :horoscope) == {:ok, "🍪s await, Taurus Mario!"}
      assert Journey.get_value(execution, :library_of_congress_record) == {:error, :not_set}
      assert Journey.get_value(execution, :birth_day) == {:ok, 26}

      execution = Journey.load(execution)

      assert Journey.values(execution) == %{
               astrological_sign: {:set, "Taurus"},
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: {:set, "Mario"},
               horoscope: {:set, "🍪s await, Taurus Mario!"},
               library_of_congress_record: :not_set
             }

      # |> Journey.wait(:library_of_congress_record)

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
      "v1.0.0",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(:astrological_sign, [:birth_month, :birth_day], fn %{birth_month: _birth_month, birth_day: _birth_day} ->
          Process.sleep(1000)
          {:ok, "Taurus"}
        end),
        compute(:horoscope, [:first_name, :astrological_sign], fn %{first_name: name, astrological_sign: sign} ->
          Process.sleep(1000)
          {:ok, "🍪s await, #{sign} #{name}!"}
        end),
        compute(
          :library_of_congress_record,
          [:horoscope, :first_name],
          fn %{
               horoscope: _horoscope,
               first_name: first_name
             } ->
            Process.sleep(1000)
            {:error, "lol no, #{first_name}."}
          end
        )
      ],
      []
    )
  end
end
