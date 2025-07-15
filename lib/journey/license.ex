defmodule Journey.License.Functions do
  @moduledoc false

  @license_key_env_var_name "JOURNEY_BUILD_KEY"
  @license_key_skip_verification_env_var_name "JOURNEY_BUILD_KEY_SKIP_VERIFICATION"

  def validate() do
    @license_key_env_var_name
    |> System.get_env()
    |> case do
      nil ->
        IO.puts("👋 Journey license key is not found (#{@license_key_env_var_name} environment var is not set).")
        IO.puts("")
        print_license_info()
        :not_set

      key ->
        validate_key(key)
    end
  end

  defp validate_key(build_key) do
    license_validation_enabled? =
      @license_key_skip_verification_env_var_name
      |> System.get_env()
      |> is_nil()

    result =
      if license_validation_enabled? do
        :inets.start()
        :ssl.start()

        :httpc.request(:get, {"https://gojourney.dev/api/validation/key/#{build_key}", []}, [{:timeout, 1000}], [])
        |> case do
          # {:ok, {{~c"HTTP/1.1", 200, ~c"OK"}, _headers, _body}} ->
          {:ok, {{_, 200, _}, _headers, body}} ->
            body
            |> List.to_string()
            |> status_from_body()

          {:error, _} ->
            :network_error

          _ ->
            :verification_error
        end
      else
        IO.puts(
          "🙈 Journey: license key validating skipped (due to #{@license_key_skip_verification_env_var_name} environment variable)."
        )

        :verification_skipped
      end

    result |> message_from_response() |> IO.puts()
    IO.puts("")
    print_license_info()
    result
  end

  defp print_license_info() do
    IO.puts("""
    🚀 Journey is free for non-commercial and small projects (≤$10k/month in revenue).

    🔑 All other uses require a build key. Get yours at https://gojourney.dev | See LICENSE.md for full terms.

    🙏 Thank you for using Journey!
    """)
  end

  # Using basic string matching instead of JSON parsing to reduce dependencies,
  # since this code executes at build time.
  defp status_from_body(body) when is_binary(body) do
    cond do
      String.contains?(body, "key_valid") ->
        :ok

      String.contains?(body, "key_expired") ->
        :key_expired

      String.contains?(body, "user_inactive") ->
        :user_inactive

      String.contains?(body, "key_inactive") ->
        :key_inactive

      String.contains?(body, "key_invalid") ->
        :key_invalid

      true ->
        :unable_to_verify_build_key
    end
  end

  defp message_from_response(:ok), do: "🙏 Journey: Thank you for supporting Journey development with a valid build key!"

  defp message_from_response(:key_expired),
    do:
      "❓ journey: the supplied build key (provided via #{@license_key_env_var_name} env var) has expired. renew or get a new key at https://gojourney.dev"

  defp message_from_response(:user_inactive),
    do:
      "❓ Journey: The owner of the supplied build key (provided via #{@license_key_env_var_name} env var) is inactive. Get a new key at https://gojourney.dev"

  defp message_from_response(:key_inactive),
    do:
      "❓ Journey: The owner of the supplied build key (provided via #{@license_key_env_var_name} env var) is inactive. Get a new key at https://gojourney.dev"

  defp message_from_response(:verification_skipped),
    do:
      "❓ Journey: Skipping build key verification (because #{@license_key_skip_verification_env_var_name} is set to true). A Journey Build Key can be obtained at  https://gojourney.dev and provided to the build via #{@license_key_env_var_name} env var."

  defp message_from_response(:key_invalid),
    do:
      "❓ Journey: The build key (provided via #{@license_key_env_var_name} env var) is not valid. Get a build key at https://gojourney.dev"

  defp message_from_response(:network_error),
    do:
      """
      ❓ Journey: Unable to verify your build key due to a network error. You can still use Journey, but please ensure you have a valid build key. https://gojourney.dev

      ℹ️ Journey: You can disable this validation by setting the #{@license_key_skip_verification_env_var_name} environment variable to 'true'.
      """
      |> String.trim()

  defp message_from_response(status),
    do:
      "❓ Journey: Unable to verify the build key (provided via #{@license_key_env_var_name} env var). Manage your build keys at https://gojourney.dev (status: #{inspect(status)}"
end

defmodule Journey.License do
  @moduledoc false

  @validation_result Journey.License.Functions.validate()

  def license_info do
    @validation_result
  end
end
