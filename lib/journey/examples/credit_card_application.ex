defmodule Journey.Examples.CreditCardApplication do
  @moduledoc """
  This module demonstrates building a simple credit card application workflow using the Journey library.

  You might find it interesting to read the actual source code of this example (the definition of the graph, and the "business logic" functions), but here is a doctest illustrating executing a credit card application workflow.


  ## Examples:

  ```elixir
  iex> # The customer starts the application process and provides their personal information.
  iex> import Journey.Node
  iex> graph = Journey.Examples.CreditCardApplication.graph()
  iex> execution = Journey.start_execution(graph)
  iex>
  iex> # This is only needed in a test, to perform background processing that happens automatically outside of tests.
  iex> background_sweeps_task = Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test(execution.id)
  iex>
  iex> execution = execution |> Journey.set_value(:full_name, "Mario")
  iex> execution = execution |> Journey.set_value(:birth_date, "10/11/1981")
  iex> execution = execution |> Journey.set_value(:ssn, "123-45-6789")
  iex> execution = execution |> Journey.set_value(:email_address, "mario@example.com")
  iex>
  iex> # This kicks off the pre-approval process, which eventually completes.
  iex> execution |> Journey.get_value(:preapproval_process_completed, wait_any: true)
  {:ok, true}
  iex> # We haven't heard from the customer, so we'll send a reminder in a few days (seconds;).
  iex> execution |> Journey.get_value(:send_preapproval_reminder, wait_any: true)
  {:ok, true}
  iex>
  iex> # Reminded, the customer requests an actual credit card.
  iex> _execution = execution |> Journey.set_value(:credit_card_requested, true)
  iex> # ... which triggers issuing the card.
  iex>
  iex> execution |> Journey.get_value(:initiate_credit_card_issuance, wait_any: true)
  {:ok, true}
  iex> execution |> Journey.values() |> redact([:schedule_request_credit_card_reminder, :execution_id, :last_updated_at])
  %{
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
      schedule_request_credit_card_reminder: 1234567890,
      execution_id: "...",
      last_updated_at: 1234567890
    }
  iex>
  iex> # Eventually, the fulfillment department marks the credit card as mailed.
  iex> # Which triggers an email notifying the customer that the card has been mailed.
  iex> execution = execution |> Journey.set_value(:credit_card_mailed, true)
  iex> execution |> Journey.get_value(:credit_card_mailed_notification, wait_any: true)
  {:ok, true}
  iex> {:ok, _} = execution |> Journey.get_value(:archive, wait_any: true)
  iex> # This is only needed in tests.
  iex> Journey.Scheduler.Background.Periodic.stop_background_sweeps_in_test(background_sweeps_task)

  ```


  """

  defmodule Compute do
    @moduledoc """
    This module contains the business logic for the Credit Card Approval application, things like fetching the customer's credit score, making and communication the credit decision, etc.
    """

    require Logger

    @doc """
    This function simulates fetching a credit score from an external service.
    """
    def fetch_credit_score(%{birth_date: _birth_date, ssn: _ssn, full_name: _full_name} = values) do
      Logger.info("fetch_credit_score: starting. for #{inspect(redact(values, :ssn))}")
      Process.sleep(1000)
      # 300 + :rand.uniform(550)
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
      # Example: make an API call to a credit cart fulfillment service.
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
        schedule_once(
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
        schedule_once(
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

  @doc false
  def test_run() do
    g = graph()

    e =
      g
      |> Journey.start_execution()
      |> Journey.set_value(:full_name, "Mario")
      |> Journey.set_value(:birth_date, "10/11/1981")
      |> Journey.set_value(:ssn, "123-45-6789")
      |> Journey.set_value(:email_address, "mario@example.com")

    {g, e}
  end
end
