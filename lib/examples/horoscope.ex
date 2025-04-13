defmodule Journey.Examples.Horoscope do
  @moduledoc false

  import Journey

  def graph() do
    Journey.new_graph(
      "horoscope workflow, success #{__MODULE__}",
      "v1.0.0",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(
          :astrological_sign,
          [:birth_month, :birth_day],
          fn %{birth_month: _birth_month, birth_day: _birth_day} ->
            Process.sleep(1000)
            {:ok, "Taurus"}
          end
        ),
        compute(
          :horoscope,
          [:first_name, :astrological_sign],
          fn %{first_name: name, astrological_sign: sign} ->
            Process.sleep(1000)
            {:ok, "🍪s await, #{sign} #{name}!"}
          end
        ),
        compute(
          :library_of_congress_record,
          [:horoscope, :first_name],
          fn %{
               horoscope: _horoscope,
               first_name: first_name
             } ->
            Process.sleep(1000)
            {:ok, "#{first_name}'s horoscope was submitted for archival."}
          end
        )
      ],
      []
    )
  end
end
