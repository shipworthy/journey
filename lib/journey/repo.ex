defmodule Journey.Repo do
  use Ecto.Repo,
    otp_app: :journey,
    adapter: Ecto.Adapters.Postgres
end
