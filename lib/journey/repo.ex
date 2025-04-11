defmodule Journey.Repo do
  @moduledoc false

  use Ecto.Repo,
    otp_app: :journey,
    adapter: Ecto.Adapters.Postgres
end
