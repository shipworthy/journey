defmodule Journey.License.Functions do
  @license_key_env_var_name "JOURNEY_LICENSE_KEY"
  @license_key_skip_verification_env_var_name "JOURNEY_LICENSE_KEY_SKIP_VERIFICATION"

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

  defp validate_key(_key) do
    license_validation_enabled? =
      @license_key_skip_verification_env_var_name
      |> System.get_env()
      |> is_nil()

    if license_validation_enabled? do
      :inets.start()
      :ssl.start()

      :httpc.request(:get, {"https://google.com", []}, [{:timeout, 1000}], [])
      |> case do
        # {:ok, {{~c"HTTP/1.1", 200, ~c"OK"}, _headers, _body}} ->
        {:ok, {{_, 202, _}, _headers, _body}} ->
          IO.puts("🙏🏽 Journey: Thank you for supporting Journey development with a valid license key!")
          :ok

        {:ok, {{_, 200, _}, _headers, _body}} ->
          IO.puts(
            "❓ Journey: The supplied license key (provided via #{@license_key_env_var_name} env var) is not valid. https://gojourney.dev"
          )

          IO.puts("")
          print_license_info()
          :not_valid

        {:error, _} ->
          IO.puts(
            "❓ Journey: Unable to verify your license key due to a network error. You can still use Journey, but please ensure you have a valid license key. https://gojourney.dev "
          )

          IO.puts(
            "ℹ️ Journey: You can disable this validation by setting the #{@license_key_skip_verification_env_var_name} environment variable to 'true'."
          )

          IO.puts("")
          print_license_info()
          :verification_error_network_error

        _ ->
          IO.puts(
            "❓ Journey: Unable to verify your license key due to an error. You can still use Journey, but please ensure you have a valid license key. https://gojourney.dev "
          )

          IO.puts(
            "ℹ️ Journey: You can disable this validation by setting the #{@license_key_skip_verification_env_var_name} environment variable to 'true'."
          )

          IO.puts("")
          print_license_info()
          :verification_error
      end
    else
      IO.puts(
        "🙈 Journey: license key validating skipped (due to #{@license_key_skip_verification_env_var_name} environment variable)."
      )

      IO.puts("")
      print_license_info()
      :verification_skipped
    end
  end

  defp print_license_info() do
    IO.puts("""
    🚀 Journey is free for individuals and small teams (≤2 engineers and ≤$10k/month in revenue).

    🔑 All other uses require a license (https://gojourney.dev). See LICENSE.md for full terms.

    🙏🏽 Thank you for using Journey!
    """)
  end
end

defmodule Journey.License do
  @validation_result Journey.License.Functions.validate()

  def license_info do
    @validation_result
  end
end
