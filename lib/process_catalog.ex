defmodule Journey.ProcessCatalog do
  use Agent

  @moduledoc false

  @spec start_link(any) :: {:error, any} | {:ok, pid}
  @doc """
  Starts a new process store.
  """
  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @spec get(String.t()) :: Journey.Process.t() | nil
  @doc """
  Gets a process by process id.
  """
  def get(process_id) do
    Agent.get(__MODULE__, fn state ->
      Map.get(state, process_id)
    end)
  end

  @spec register(%Journey.Process{}) :: Journey.Process.t()
  @doc """
  Stores a process.
  """
  def register(process) do
    :ok =
      Agent.update(__MODULE__, fn storage ->
        # execution = %{execution | save_version: execution.save_version + 1}
        Map.put(storage, process.process_id, process)
      end)

    get(process.process_id)
  end
end
