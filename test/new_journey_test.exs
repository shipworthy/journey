defmodule NewJourneyTest do
  use ExUnit.Case
  doctest NewJourney

  test "greets the world" do
    assert NewJourney.hello() == :world
  end
end
