defmodule Flows.Test.HoroscopeGraph do
  @moduledoc false
  import Journey.Node

  import Journey.Node.UpstreamDependencies

  def create_graph() do
    Journey.new_graph(
      "horoscope workflow, success #{__MODULE__}",
      "v1.0.0",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(:astrological_sign, [:birth_month, :birth_day], &compute_sign/1),
        compute(:horoscope, unblocked_when([:first_name, :astrological_sign]), &compute_horoscope/1),
        compute(
          :library_of_congress_record,
          unblocked_when([:horoscope, :first_name]),
          fn %{
               horoscope: _horoscope,
               first_name: first_name
             } ->
            Process.sleep(1000)
            {:ok, "#{first_name}'s horoscope was submitted for archival."}
          end
        ),
        mutate(
          :obfuscate_first_name,
          unblocked_when([:first_name, :library_of_congress_record]),
          fn %{first_name: first_name} ->
            encrypted_first_name = first_name |> String.graphemes() |> Enum.reverse() |> Enum.join("")
            {:ok, encrypted_first_name}
          end,
          mutates: :first_name
        )
      ]
    )
  end

  defp compute_sign(%{birth_month: _birth_month, birth_day: _birth_day}) do
    Process.sleep(1000)
    {:ok, "Taurus"}
  end

  defp compute_horoscope(%{first_name: name, astrological_sign: sign}) do
    Process.sleep(1000)
    {:ok, "🍪s await, #{sign} #{name}!"}
  end
end

defmodule Flows.HoroscopeTest do
  use ExUnit.Case, async: true

  import Journey.Node

  describe "flow" do
    test "sunny day" do
      execution =
        Flows.Test.HoroscopeGraph.create_graph()
        |> Journey.start_execution()
        |> Journey.set(:birth_day, 26)
        |> Journey.set(:birth_month, "April")
        |> Journey.set(:first_name, "Mario")

      assert Journey.values_all(execution) |> redact([:execution_id, :last_updated_at]) == %{
               astrological_sign: :not_set,
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: {:set, "Mario"},
               horoscope: :not_set,
               library_of_congress_record: :not_set,
               obfuscate_first_name: :not_set,
               execution_id: {:set, "..."},
               last_updated_at: {:set, 1_234_567_890}
             }

      assert Journey.get_value(execution, :astrological_sign) == {:error, :not_set}
      assert Journey.get_value(execution, :horoscope) == {:error, :not_set}
      assert Journey.get_value(execution, :library_of_congress_record) == {:error, :not_set}
      assert {:ok, %{value: 26}} = Journey.get_value(execution, :birth_day)

      assert {:ok, %{value: "Taurus"}} = Journey.get_value(execution, :astrological_sign, wait_any: true)
      assert {:ok, %{value: "🍪s await, Taurus Mario!"}} = Journey.get_value(execution, :horoscope, wait_any: true)

      assert {:ok, %{value: "Mario's horoscope was submitted for archival."}} =
               Journey.get_value(execution, :library_of_congress_record, wait_any: :infinity)

      assert {:ok, %{value: "updated :first_name"}} =
               Journey.get_value(execution, :obfuscate_first_name, wait_any: true)

      execution = Journey.load(execution)

      assert Journey.values_all(execution) |> redact([:execution_id, :last_updated_at]) == %{
               astrological_sign: {:set, "Taurus"},
               birth_day: {:set, 26},
               birth_month: {:set, "April"},
               first_name: {:set, "oiraM"},
               horoscope: {:set, "🍪s await, Taurus Mario!"},
               library_of_congress_record: {:set, "Mario's horoscope was submitted for archival."},
               obfuscate_first_name: {:set, "updated :first_name"},
               execution_id: {:set, "..."},
               last_updated_at: {:set, 1_234_567_890}
             }
    end
  end
end
