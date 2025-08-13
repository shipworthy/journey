# Journey

Journey is an Elixir library for building and executing computation graphs with built-in persistence, reliability, and scalability.

Define your application workflows as dependency graphs where user inputs automatically trigger computations in the correct order, with all state persisted to PostgreSQL. Your flows survive crashes, redeploys, page reloads, while scaling naturally with your application - no additional infrastructure or cloud service$ required.

## Installation

The package can be installed by adding `journey` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:journey, "~> 0.10"}
  ]
end
```

Documentation can be found at <https://shipworthy.hexdocs.pm/journey/>.
