defmodule Journey.Graph do
  defstruct [:name, :inputs_and_steps]
  @type t :: %__MODULE__{name: String.t(), inputs_and_steps: list}

  def new(name, inputs_and_steps) when is_binary(name) and is_list(inputs_and_steps) do
    %__MODULE__{
      name: name,
      inputs_and_steps: inputs_and_steps
    }
  end

  def new(name, inputs_and_steps) when is_binary(name) and is_list(inputs_and_steps) do
    %__MODULE__{
      name: name,
      inputs_and_steps: inputs_and_steps
    }
  end
end
