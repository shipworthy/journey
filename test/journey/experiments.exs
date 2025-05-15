defmodule Journey.ExperimentsScripts do
  import Journey
  alias Journey.Execution
  alias Journey.Graph
  alias Journey.Helpers.Random

  _ = """

  execution:
    id,
    graph_name,
    revision,
    nodes

  value:
    id,
    execution_id,
    name,
    value,
    ex_revision,
    data schema (optional)

  computation:
    id,
    execution_id,
    name,
    scheduled_time_at,
    picked up at,
    completed at,
    deadline,
    state,
    error details,
    ex_revision_at_start,
    ex_revision_at_completion


    is execution version needed?

    advocacy:

    ----------
    reminders_graph = Graph.new(
      "reminders",
      [
        input(:name),
        input(:email),
        schedule_recurring(:remind_to_advocate_schedule, [], fn _ ->
          {:ok, System.system_time(:second) + one_week_in_seconds()}
        end),
        computation(:send_notification, [:name, :email, :remind_to_advocate_schedule], fn %{name: name, email: email} ->
          send_reminder(name, email)
          {:ok, true}
        end)
        ]
    )

    advocacy_graph = Graph.new(
      "advocacy",
      [
        input(:name),
        input(:zip),
        compute(:possible_districts, [:zip], fn %{zip: zip} ->
          {:ok, get_possible_districts(zip)}
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
        pulse(:hash_pii_schedule, [:name], fn %{name: name} ->
          {:ok, System.system_time(:second) + one_day_in_seconds()}
        end),
        value(:email_address),
        value(:please_remind_me),
        compute(
          :create_reminder,
          [:name, :email_address, :please_remind_me],
          fn %{name: name, email_address: email_address, please_remind_me: please_remind_me} ->
            if please_remind_me do
              # Create an execution for the "reminder" graph
              reminder_execution =
                reminders_graph
                |> Journey.start_execution()
                |> Journey.set_value(:name, name)
                |> Journey.set_value(:email, email_address)
              {:ok, reminder_execution_id}
            else
              # TODO: figure out how to turn off reminders.
              {:ok, nil}
            end
          end)
        ),
        mutate(:name, [:name, :encrypt_pii_schedule], fn %{name: name} ->
          {:ok, hash(name)}
        end)
      ]
    )

    # graph(
    #   "reminders",
    #   [
    #     input(:name),
    #     input(:email),
    #     pulse(:remind_to_advocate_schedule, [], fn _ ->
    #       scheduled_time = System.system_time(:second) + one_week_in_seconds()
    #       {:ok, scheduled_time}
    #     end),
    #     computation(:send_notification, [:name, :email, :remind_to_advocate_schedule], fn name, email, notification_period ->
    #       send_reminder(name, email)
    #       {:ok, true}
    #     end)
    #   ]


    ooshki:

    graph(
    "ooshki",
    [
      input(:visibility),
      input(:state),
      input(:contents),
      input(:owners),
      input(:recipients),
      input(:timeline),
      input(:notes),
      input(:notification_configuration),
      input(:notify_of_changes)
      input(:notification_queue)

      step(:step1, [:input1], fn input1 -> input1 end),
      step(:step2, [:input2], fn input2 -> input2 end)
    ]
    )

  """

  user_onboarding_graph_separate_mutate =
    Graph.new(
      "user_onboarding",
      "v123",
      [
        input(:name),
        schedule_once(:cleanup_pii, [], fn _ ->
          {:ok, System.system_time(:second) + 86_400}
        end),
        compute(
          :welcome_message,
          [:name],
          fn %{name: name} ->
            {:ok, "Hi #{name}, welcome!"}
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

  user_onboarding_graph_mutate_as_compte =
    Graph.new(
      "user_onboarding",
      "v123",
      [
        input(:name),
        schedule_once(:cleanup_pii_schedule, [], fn _ ->
          {:ok, System.system_time(:second) + 86_400}
        end),
        compute(
          :welcome_message,
          [:name],
          fn %{name: name} ->
            {:ok, "Hi #{name}, welcome!"}
          end,
          max_retries: 3,
          backoff_strategy_ms: [1000, 2000, 3000],
          consider_abandoned_after_ms: 30_000
        ),
        compute(
          :replace_name_with_hash,
          [:name, :cleanup_pii_schedule],
          fn %{name: name} ->
            {:ok, hash(name)}
          end,
          mutates: :name
        )
      ]
    )

  user_onboarding_graph_mutate_as_a_computation =
    Graph.new(
      "user_onboarding",
      "v123",
      [
        input(:name),
        schedule_once(:cleanup_pii_schedule, [], fn _ ->
          {:ok, System.system_time(:second) + 86_400}
        end),
        compute(
          :welcome_message,
          [:name],
          fn %{name: name} ->
            {:ok, "Hi #{name}, welcome!"}
          end,
          max_retries: 3,
          backoff_strategy_ms: [1000, 2000, 3000],
          consider_abandoned_after_ms: 30_000
        )
        # mutate(
        #   :replace_name_with_hash,
        #   [:name, :cleanup_pii_schedule],
        #   fn %{name: name} ->
        #     {:ok, hash(name)}
        #   end,
        #   mutates: :name
        #   # same retry options apply
        # )
      ]
    )

  reminders_graph =
    Graph.new(
      "reminders",
      "v2",
      [
        input(:name),
        input(:email),
        input(:enabled),
        schedule_recurring(:remind_to_advocate_schedule, [:enabled], fn
          %{enabled: true} -> {:ok, System.system_time(:second) + one_week_in_seconds()}
          %{enabled: false} -> {:ok, nil}
        end),
        compute(:send_notification, [:name, :email, :remind_to_advocate_schedule], fn %{name: name, email: email} ->
          send_reminder(name, email)
          {:ok, true}
        end)
      ]
    )

  advocacy_graph =
    Graph.new(
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
        schedule_once(:hash_pii_schedule, [:name], fn %{name: name} ->
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
      ]
      # [
      #   mutate(:name, [:name, :hash_pii_schedule], fn name ->
      #     {:ok, hash(name)}
      #   end)
      # ]
    )

  advocacy_execution =
    advocacy_graph
    |> Journey.start_execution()
    |> Journey.set_value(:name, "Mario")
    |> Journey.set_value(:zip, "12345")
    |> Journey.set_value(:district, "District 1")
    |> Journey.set_value(:email_address, "mario@bowser.castle")
    |> Journey.set_value(:please_remind_me, true)

  def approved?(decision), do: decision == :approved
  def declined?(decision), do: decision == :declined

  mortgage_graph =
    Graph.new(
      "mortgage",
      "88de90b4-3f2c-4a0e-8d1b-5f7a6c9d3e2f",
      [
        input(:name),
        input(:zip),
        input(:ssn),
        input(:address),
        compute(:credit_score, [:name, :address, :ssn], fn %{name: name, ssn: ssn, address: address} ->
          credit_score = compute_credit_score(name, ssn, address)
          {:ok, credit_score}
        end),
        compute(:decision, [:credit_score], fn %{credit_score: credit_score} ->
          {:ok, compute_decision(credit_score)}
        end),
        compute(
          :congrats,
          [:name, :address, decision: &approved?/1],
          fn %{name: name, address: address} ->
            {:ok, send_congrats_mail(name, address)}
          end
        ),
        compute(
          :so_sorry,
          [:name, :address, decision: &declined?/1],
          fn %{name: name, address: address} ->
            {:ok, send_so_sorry_mail(name, address)}
          end
        )
      ]
      # [
      #   mutate(:ssn, [:decision], fn ssn -> {:ok, hash(ssn)} end),
      #   mutate(:credit_score, [:decision], fn credit_score -> {:ok, hash(credit_score)} end)
      # ]
    )

  zodiac =
    Journey.new_graph(
      "horoscope workflow",
      "v1.0.0",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(
          :zodiac_sign,
          [:birth_month, :birth_day],
          fn %{birth_month: _birth_month, birth_day: _birth_day} ->
            Process.sleep(1000)
            {:ok, "Taurus"}
          end
        ),
        compute(
          :horoscope,
          [:first_name, :zodiac_sign],
          fn %{first_name: name, zodiac_sign: zodiac_sign} ->
            Process.sleep(1000)
            {:ok, "🍪s await, #{zodiac_sign} #{name}!"}
          end
        )
      ]
    )
end
