defmodule Journey.Examples.CreditCardApplication do
  @moduledoc """
  This module ([lib/journey/examples/credit_card_application.ex](https://github.com/shipworthy/journey/blob/main/lib/journey/examples/credit_card_application.ex)) contains an example of a credit card application workflow built with Journey.

  The customer provides their personal information, which kicks off a pre-approval process (fetching a credit score, making and communicating the decision, and redacting the SSN once it is no longer needed). Pre-approved customers who haven't requested a card get a scheduled reminder. Requesting a card initiates its issuance, and once the card is mailed, the customer is notified and the execution is archived.

  The graph is defined in `graph/0`. For a step-by-step walkthrough of running it, see the [Credit Card Application livebook](lib/examples/credit_card_application.livemd).
  """

  defmodule Compute do
    @moduledoc """
    This module contains the business logic for the Credit Card Approval application, things like fetching the customer's credit score, making and communicating the credit decision, etc.
    """

    require Logger

    @doc """
    This function simulates fetching a credit score from an external service.
    """
    def fetch_credit_score(%{birth_date: _birth_date, ssn: _ssn, full_name: _full_name} = values) do
      Logger.info("fetch_credit_score: starting. for #{inspect(redact(values, :ssn))}")
      Process.sleep(1000)
      credit_score = 800
      Logger.info("fetch_credit_score: completed. result: #{credit_score}")
      {:ok, credit_score}
    end

    @doc """
    This function simulates computing the credit decision, based on the credit score.
    """
    def compute_decision(%{credit_score: credit_score}) do
      Logger.info("compute_decision: starting. Score: #{credit_score}")
      Process.sleep(1000)
      decision = if credit_score > 700, do: :approved, else: :rejected
      Logger.info("compute_decision: finished. Decision: #{inspect(decision)}")
      {:ok, decision}
    end

    @doc """
    This function simulates sending the customer an email when their application was approved.
    """
    def send_congrats(values) do
      Logger.info("send_congrats: starting. values #{inspect(values)}")
      Process.sleep(1000)
      Logger.info("send_congrats: finished.")
      {:ok, :email_sent_congrats}
    end

    @doc """
    This function simulates sending the customer an email when their application was declined.
    """
    def send_rejection(values) do
      Logger.info("send_rejection: starting. values #{inspect(values)}")
      Process.sleep(1000)
      Logger.info("send_rejection: finished.")
      {:ok, :email_sent_rejection}
    end

    @doc """
    This function simulates scheduling sending a reminder to preapproved customers.
    """
    def choose_the_time_to_send_reminder(values) do
      Logger.info("choose_the_time_to_send_reminder: starting. values #{inspect(values)}")
      when_to_send_reminder = System.system_time(:second) + 6
      as_dt = DateTime.from_unix!(when_to_send_reminder)
      Logger.info("choose_the_time_to_send_reminder: to be sent at #{as_dt}.")
      {:ok, when_to_send_reminder}
    end

    @doc """
    This function simulates sending the preapproved customer a reminder to request a credit card.
    """
    def send_preapproval_reminder(values) do
      Logger.info("send_preapproval_reminder: starting. values #{inspect(values)}")
      Process.sleep(1000)
      Logger.info("send_preapproval_reminder: finished.")
      {:ok, true}
    end

    @doc """
    This function simulates initiating the issuance and mailing of a credit card.
    """
    def request_credit_card_issuance(values) do
      Logger.info("request_credit_card_issuance: starting. values #{inspect(values)}")
      Process.sleep(1000)
      # Example: make an API call to a credit card fulfillment service.
      Logger.info("request_credit_card_issuance: finished.")
      {:ok, true}
    end

    @doc """
    This function simulates emailing the customer and telling them that the card has been mailed.
    """
    def send_card_mailed_notification(values) do
      Logger.info("send_card_mailed_notification: starting. values #{inspect(values)}")
      Process.sleep(1000)
      # Example: send an email to the customer telling them that the card is in the mail.
      Logger.info("send_card_mailed_notification: finished.")
      {:ok, true}
    end

    @doc """
    This function marks the flow as completed when it's all done.
    """
    def all_done(values) do
      Logger.info("all_done: starting. values #{inspect(values)}")
      Process.sleep(1000)
      Logger.info("all_done: finished.")
      {:ok, true}
    end

    @doc """
    This function schedules archiving the execution.
    """
    def choose_the_time_to_archive(values) do
      Logger.debug("choose_the_time_to_archive: starting. values #{inspect(values)}")
      when_to_archive = System.system_time(:second) + 5
      as_dt = DateTime.from_unix!(when_to_archive)
      Logger.debug("choose_the_time_to_archive: to be archived at #{as_dt}.")
      {:ok, when_to_archive}
    end

    defp redact(map, key) when is_map(map) and is_atom(key) do
      if Map.has_key?(map, key) do
        Map.put(map, key, "...")
      else
        map
      end
    end
  end

  defp long_time_since_last_update?(%{node_value: last_updated_at, execution_id: _execution_id}) do
    System.system_time(:second) - last_updated_at > 60 * 60 * 24
  end

  require Logger

  import Journey.Node
  import Journey.Node.Conditions
  import Journey.Node.UpstreamDependencies

  @doc """
  This function defines the graph for the credit card application workflow.

  The graph is defined as a list of nodes.
  Input nodes have a name.
  Computation nodes also have upstream dependencies and a function to compute the node's value, and a few other options.
  """
  def graph() do
    Journey.new_graph(
      "Credit Card Application flow graph",
      "v1.0.0",
      [
        input(:full_name),
        input(:birth_date),
        input(:ssn),
        input(:email_address),
        compute(:credit_score, [:full_name, :birth_date, :ssn, :email_address], &Compute.fetch_credit_score/1),
        mutate(:ssn_redacted, [:credit_score], fn _ -> {:ok, "<redacted>"} end, mutates: :ssn),
        compute(:preapproval_decision, [:credit_score, :full_name], &Compute.compute_decision/1),
        compute(:congratulate, unblocked_when({:preapproval_decision, &approved?/1}), &Compute.send_congrats/1),
        compute(
          :inform_of_rejection,
          unblocked_when({:preapproval_decision, &rejected?/1}),
          &Compute.send_rejection/1
        ),
        compute(
          :preapproval_process_completed,
          unblocked_when({:or, [{:congratulate, &provided?/1}, {:inform_of_rejection, &provided?/1}]}),
          &Compute.all_done/1
        ),
        tick_once(
          :schedule_request_credit_card_reminder,
          [:congratulate],
          &Compute.choose_the_time_to_send_reminder/1
        ),
        input(:credit_card_requested),
        compute(
          :send_preapproval_reminder,
          unblocked_when({
            :and,
            [
              {:schedule_request_credit_card_reminder, &provided?/1},
              {:not, {:credit_card_requested, &true?/1}}
            ]
          }),
          &Compute.send_preapproval_reminder/1
        ),
        compute(
          :initiate_credit_card_issuance,
          unblocked_when({:credit_card_requested, &true?/1}),
          &Compute.request_credit_card_issuance/1
        ),
        input(:credit_card_mailed),
        compute(
          :credit_card_mailed_notification,
          unblocked_when({
            :and,
            [
              {:credit_card_mailed, &true?/1},
              {:initiate_credit_card_issuance, &provided?/1}
            ]
          }),
          &Compute.send_card_mailed_notification/1
        ),
        tick_once(
          :schedule_archival,
          unblocked_when({
            :or,
            [
              {:last_updated_at, &long_time_since_last_update?/1},
              {:or, [{:credit_card_mailed_notification, &provided?/1}, {:inform_of_rejection, &provided?/1}]}
            ]
          }),
          &Compute.choose_the_time_to_archive/1
        ),
        archive(:archive, [:schedule_archival])
      ]
    )
  end

  defp approved?(%{node_value: value} = _credit_decision_node) do
    Logger.debug("approved?: starting. Value: #{value}")
    result = value == "approved"
    Logger.debug("approved?: completed. result: #{result}")
    result
  end

  defp rejected?(%{node_value: value} = _credit_decision_node) do
    Logger.debug("rejected?: starting. Value: #{value}")
    result = value == "rejected"
    Logger.debug("rejected?: completed. result: #{result}")
    result
  end
end
