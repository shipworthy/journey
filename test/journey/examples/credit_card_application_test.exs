defmodule Journey.Examples.CreditCardApplicationTest do
  use ExUnit.Case, async: true

  import Journey.Node

  alias Journey.Examples.CreditCardApplication
  alias Journey.Scheduler.Background.Periodic

  test "sunny day: pre-approval through archival" do
    execution = Journey.start(CreditCardApplication.graph())

    background_sweeps_task = Periodic.start_background_sweeps_in_test(execution.id)
    on_exit(fn -> Periodic.stop_background_sweeps_in_test(background_sweeps_task) end)

    execution =
      execution
      |> Journey.set(:full_name, "Mario")
      |> Journey.set(:birth_date, "10/11/1981")
      |> Journey.set(:ssn, "123-45-6789")
      |> Journey.set(:email_address, "mario@example.com")

    {:ok, true, _} = Journey.get(execution, :preapproval_process_completed, wait: :any)
    {:ok, true, _} = Journey.get(execution, :send_preapproval_reminder, wait: :any)

    execution = Journey.set(execution, :credit_card_requested, true)
    {:ok, true, _} = Journey.get(execution, :initiate_credit_card_issuance, wait: :any)

    assert execution
           |> Journey.values()
           |> redact([:schedule_request_credit_card_reminder, :execution_id, :last_updated_at]) == %{
             preapproval_process_completed: true,
             birth_date: "10/11/1981",
             congratulate: "email_sent_congrats",
             preapproval_decision: "approved",
             credit_score: 800,
             email_address: "mario@example.com",
             full_name: "Mario",
             ssn: "<redacted>",
             ssn_redacted: "updated :ssn",
             credit_card_requested: true,
             initiate_credit_card_issuance: true,
             schedule_request_credit_card_reminder: 1_234_567_890,
             execution_id: "...",
             last_updated_at: 1_234_567_890
           }

    execution = Journey.set(execution, :credit_card_mailed, true)
    {:ok, true, _} = Journey.get(execution, :credit_card_mailed_notification, wait: :any)

    {:ok, _archived_at, _} = Journey.get(execution, :archive, wait: :any)
    assert Journey.load(execution.id) == nil
    assert Journey.load(execution.id, include_archived: true) != nil
  end
end
