defmodule Journey.Test.ProcessCatalog do
  use ExUnit.Case
  doctest Journey

  @tiny_process %Journey.Process{
    process_id: "tiny test process 2",
    steps: [
      %Journey.Step{name: :first_name},
      %Journey.Step{name: :birth_month},
      %Journey.Step{name: :birth_day}
    ]
  }

  test "basic process" do
    Journey.ProcessCatalog.register(@tiny_process)
    assert Journey.ProcessCatalog.get(@tiny_process.process_id) == @tiny_process
  end
end
