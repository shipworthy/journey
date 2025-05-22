defmodule Journey.Examples.CreditCardApplication do
  @moduledoc """
  This module demonstrates building a simple credit card application workflow using the Journey library.

  ## Examples:

  ```elixir
  iex> graph = Journey.Examples.CreditCardApplication.graph()
  iex> execution = Journey.start_execution(graph)
  iex> execution = execution |> Journey.set_value(:full_name, "Mario")
  iex> execution = execution |> Journey.set_value(:birth_date, "10/11/1981")
  iex> execution = execution |> Journey.set_value(:ssn, "123-45-6789")
  iex> execution = execution |> Journey.set_value(:email_address, "mario@example.com")
  iex> execution |> Journey.get_value(:all_done, wait: true)
  {:ok, true}
  iex> execution |> Journey.values()
  %{
      all_done: true,
      birth_date: "10/11/1981",
      congratulate: "email_sent_congrats",
      credit_decision: "approved",
      credit_score: 800,
      email_address: "mario@example.com",
      full_name: "Mario",
      ssn: "<redacted>",
      ssn_redacted: "updated :ssn"
    }

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
      Logger.info("fetch_credit_score: starting. for #{inspect(redact_if_exists(values, :ssn))}")
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
    This function marks the flow as completed when it's all done.
    """
    def all_done(values) do
      Logger.info("all_done: starting. values #{inspect(values)}")
      Process.sleep(1000)
      Logger.info("all_done: finished.")
      {:ok, true}
    end

    defp redact_if_exists(map, key) when is_map(map) and is_atom(key) do
      if Map.has_key?(map, key) do
        Map.put(map, key, "<redacted>")
      else
        map
      end
    end
  end

  require Logger

  import Journey.Node
  import Journey.Node.UpstreamDependencies

  def graph() do
    Journey.new_graph(
      "credit card application graph (#{__MODULE__}-#{Journey.Helpers.Random.random_string(5)})",
      "v1.0.0",
      [
        input(:full_name),
        input(:birth_date),
        input(:ssn),
        input(:email_address),
        compute(:credit_score, [:full_name, :birth_date, :ssn, :email_address], &Compute.fetch_credit_score/1),
        mutate(:ssn_redacted, [:ssn], fn _ -> {:ok, "<redacted>"} end, mutates: :ssn),
        compute(:credit_decision, [:credit_score, :full_name], &Compute.compute_decision/1),
        compute(:congratulate, unblocked_when({:credit_decision, &approved?/1}), &Compute.send_congrats/1),
        compute(
          :inform_of_rejection,
          unblocked_when(
            {:and,
             [
               {:credit_decision, &provided?/1},
               {:credit_decision, &rejected?/1}
             ]}
          ),
          &Compute.send_rejection/1
        ),
        compute(
          :all_done,
          unblocked_when({:or, [{:congratulate, &provided?/1}, {:inform_of_rejection, &provided?/1}]}),
          &Compute.all_done/1
        )
      ]
    )
  end

  defp approved?(%{node_value: value} = _credit_decision_node) do
    Logger.info("approved?: starting. Value: #{value}")
    result = value == "approved"
    Logger.info("approved?: completed. result: #{result}")
    result
  end

  defp rejected?(%{node_value: value} = _credit_decision_node) do
    Logger.info("rejected?: starting. Value: #{value}")
    result = value == "rejected"
    Logger.info("rejected?: completed. result: #{result}")
    result
  end

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
