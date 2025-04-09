defmodule Journey.Graph do
  defstruct [:name, :version, :inputs_and_steps, :mutations]
  @type t :: %__MODULE__{name: String.t(), inputs_and_steps: list}

  def new(name, version, inputs_and_steps, mutations)
      when is_binary(name) and is_binary(version) and is_list(inputs_and_steps) and is_list(mutations) do
    %__MODULE__{
      name: name,
      version: version,
      inputs_and_steps: inputs_and_steps,
      mutations: mutations
    }
  end
end
