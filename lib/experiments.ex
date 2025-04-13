defmodule Journey.Experiments do
  @moduledoc false

  import Journey
  import Journey.Helpers.GrabBag

  require Logger

  def experiment0_abandoned() do
    g =
      Journey.new_graph(
        "astrological sign workflow, abandoned compute #{__MODULE__}",
        "v1.0.0",
        [
          input(:birth_day),
          input(:birth_month),
          compute(
            :astrological_sign,
            [:birth_month, :birth_day],
            fn %{birth_month: _birth_month, birth_day: _birth_day} ->
              Process.sleep(:timer.seconds(20))
              {:ok, "Taurus"}
            end,
            abandon_after_seconds: 1
          )
        ],
        []
      )

    e = g |> Journey.start_execution()

    {g, e}
  end

  def experiment1() do
    user_onboarding_graph =
      Journey.new_graph(
        "user_onboarding",
        "v123",
        [
          input(:name),
          input(:zip),
          pulse_once(:cleanup_pii, [], fn _ ->
            {:ok, System.system_time(:second) + 86_400}
          end),
          compute(
            :welcome_message_compose,
            [:name, :zip],
            fn %{name: name, zip: zip} ->
              Logger.info("Welcome message computation in user_onboarding graph.")
              Process.sleep(20_000)
              {:ok, "Hi #{name} in #{zip}, welcome!"}
            end,
            max_retries: 3,
            backoff_strategy_ms: [1000, 2000, 3000],
            consider_abandoned_after_ms: 1_000
          ),
          compute(
            :welcome_message_send,
            [:welcome_message_compose],
            fn _ ->
              Logger.info("sending welcome message.")
              {:ok, :done}
            end,
            max_retries: 3,
            backoff_strategy_ms: [1000, 2000, 3000],
            consider_abandoned_after_ms: 30_000
          )
        ],
        [
          mutate(:name, [:cleanup_pii], fn name ->
            {:ok, hash(name)}
          end)
        ]
      )

    execution =
      user_onboarding_graph
      |> Journey.start_execution()

    # |> Journey.set_value(:name, "John Doe")

    {user_onboarding_graph, execution}
  end

  def experiment2() do
    graph =
      Journey.new_graph(
        "reminders",
        "v2",
        [
          input(:name),
          input(:email),
          input(:enabled),
          pulse_recurring(:remind_to_advocate_pulse, [:enabled], fn
            %{enabled: true} -> {:ok, System.system_time(:second) + one_week_in_seconds()}
            %{enabled: false} -> {:ok, nil}
          end),
          compute(:send_notification, [:name, :email, :remind_to_advocate_pulse], fn %{name: name, email: email} ->
            send_reminder(name, email)
            {:ok, true}
          end)
        ],
        []
      )

    execution =
      graph
      |> Journey.start_execution()

    {graph, execution}
  end

  def experiment3() do
    reminders_graph =
      Journey.new_graph(
        "reminders",
        "v2",
        [
          input(:name),
          input(:email),
          input(:enabled),
          pulse_recurring(:remind_to_advocate_pulse, [:enabled], fn
            %{enabled: true} -> {:ok, System.system_time(:second) + one_week_in_seconds()}
            %{enabled: false} -> {:ok, nil}
          end),
          compute(:send_notification, [:name, :email, :remind_to_advocate_pulse], fn %{name: name, email: email} ->
            send_reminder(name, email)
            {:ok, true}
          end)
        ],
        []
      )

    graph =
      Journey.new_graph(
        "advocacy",
        "v45",
        [
          input(:name),
          input(:zip),
          compute(:possible_districts, [:zip], fn %{zip: zip} ->
            districts = get_possible_districts(zip)
            {:ok, districts}
          end),
          input(:district),
          compute(:senator1, [:district], fn %{district: district} ->
            {:ok, get_senator1(district)}
          end),
          compute(:senator2, [:district], fn %{district: district} ->
            {:ok, get_senator2(district)}
          end),
          compute(:house_rep, [:district], fn %{district: district} ->
            {:ok, get_house_rep(district)}
          end),
          compute(
            :letter_to_senator1,
            [:senator1, :name, :zip, :district],
            fn %{
                 senator1: senator1,
                 name: name,
                 zip: zip,
                 district: district
               } ->
              # compose_letter raises an error in case of failure.
              {:ok, compose_letter(senator1, name, zip, district)}
            end
          ),
          pulse_once(:hash_pii_pulse, [:name], fn _ ->
            {:ok, System.system_time(:second) + one_day_in_seconds()}
          end),
          input(:email_address),
          input(:please_remind_me),
          compute(
            :reminder_workflow,
            [:name, :email_address, :please_remind_me],
            fn %{
                 name: name,
                 email_address: email_address,
                 please_remind_me: please_remind_me,
                 reminder_workflow: reminder_workflow
               } ->
              if please_remind_me do
                reminder_execution =
                  reminders_graph
                  |> Journey.start_execution()
                  |> Journey.set_value(:name, name)
                  |> Journey.set_value(:email, email_address)
                  |> Journey.set_value(:enabled, true)

                {:ok, reminder_execution.id}
              else
                if reminder_workflow == nil do
                  {:ok, nil}
                else
                  # create_reminder is the id for the corresponding reminder graph's execution
                  reminder_execution = Journey.load(reminder_workflow)
                  Journey.set_value(reminder_execution, :enabled, false)
                  {:ok, reminder_execution.id}
                end
              end
            end
          )
        ],
        [
          mutate(:name, [:name, :hash_pii_pulse], fn name ->
            {:ok, hash(name)}
          end)
        ]
      )

    execution = Journey.start_execution(graph)
    {graph, execution}
  end

  defp get_possible_districts(zip) do
    # Simulate a function that returns possible districts based on zip code
    ["District A - #{zip}", "District B - #{zip}", "District C - #{zip}"]
  end

  defp one_day_in_seconds() do
    86_400
  end

  defp one_week_in_seconds() do
    604_800
  end

  defp get_senator1(district) do
    # Simulate a function that returns senator1 based on district
    "Senator 1 of #{district}"
  end

  defp get_senator2(district) do
    # Simulate a function that returns senator1 based on district
    "Senator 2 of #{district}"
  end

  defp get_house_rep(district) do
    # Simulate a function that returns house representative based on district
    "House Rep of #{district}"
  end

  defp compose_letter(senator, name, zip, district) do
    # Simulate a function that composes a letter
    "Dear #{senator},\n\nMy name is #{name} from #{zip}, #{district}.\n\nSincerely,\n#{name}"
  end

  defp send_reminder(name, email) do
    # Simulate sending a reminder
    IO.puts("pretending to be sending reminder to #{name} at #{email}")
  end
end
