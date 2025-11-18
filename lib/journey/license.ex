defmodule Journey.License.Functions do
  @moduledoc false

  @license_key_env_var_name "JOURNEY_BUILD_KEY"
  @license_key_skip_verification_env_var_name "JOURNEY_BUILD_KEY_SKIP_VERIFICATION"
  @license_key_service_env_var_name "JOURNEY_LICENSE_KEY_SERVICE_URL"
  @license_key_service System.get_env(@license_key_service_env_var_name, "https://gojourney.dev")

  def validate() do
    license_key_service = System.get_env(@license_key_service_env_var_name, @license_key_service)
    skip_check? = System.get_env(@license_key_skip_verification_env_var_name, "false") == "true"
    build_key = System.get_env(@license_key_env_var_name, "not_set")

    print_license_info()

    if skip_check? do
      "verification_skipped"
    else
      validate_key(license_key_service, build_key)
      |> handle_not_set(build_key)
    end
    |> message_from_response()
    |> IO.puts()
  end

  defp handle_not_set(_result, "not_set"), do: "not_set"
  defp handle_not_set(result, _build_key), do: result

  defp validate_key(service_url, build_key) do
    :inets.start()
    :ssl.start()

    :httpc.request(:get, {"#{service_url}/api/validation/key/#{build_key}?format=text", []}, [{:timeout, 1000}], [])
    |> case do
      {:ok, {{_, 200, _}, _headers, body}} ->
        body
        |> List.to_string()

      {:error, _} ->
        "network_error"

      _ ->
        "verification_error"
    end
  rescue
    _e -> "unable_to_verify_build_key"
  end

  defp print_license_info() do
    IO.puts("""
    🚀 Journey is free for "small" projects (≤$10k/month in revenue).
    🔑 All other uses require a commercial build key.
    ⚙️ Set your Journey build key via #{@license_key_env_var_name} env variable (e.g. `export JOURNEY_BUILD_KEY=B...`).
    ⚙️ Get and manage your free and commercial build keys at #{keys_url()}.
    🙏 Thank you for using Journey!
    """)
  end

  defp message_from_response("valid_commercial"),
    do: "🙏 Thank you for supporting Journey development with a valid commercial build key!"

  defp message_from_response("valid"),
    do: "🙏 Thank you for supporting Journey development with a valid free build key!"

  defp message_from_response("expired"),
    do: "❓ The supplied Journey build key has expired."

  defp message_from_response("user_inactive"),
    do: "❓ The owner of the supplied Journey build key is inactive."

  defp message_from_response("inactive"),
    do: "❓ The supplied Journey build key is inactive."

  defp message_from_response("verification_skipped"),
    do:
      "❓ Skipping Journey build key verification (because #{@license_key_skip_verification_env_var_name} is set to 'true')."

  defp message_from_response("not_set"),
    do: "❓ The Journey build key is not provided."

  defp message_from_response("invalid"),
    do: "❓ The Journey build key is not valid."

  defp message_from_response("network_error"),
    do:
      """
      ❓ Unable to verify your Journey build key due to a network error.
      """
      |> String.trim()

  defp message_from_response(status),
    do: "❓ Unable to verify Journey build key. (status: #{inspect(status)})"

  defp keys_url() do
    "#{@license_key_service}/keys"
  end
end

defmodule Journey.License do
  @moduledoc false

  @validation_result Journey.License.Functions.validate()

  def license_info do
    @validation_result
  end
end
