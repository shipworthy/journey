defmodule Journey do
  @moduledoc ~S"""
  Journey helps you define and execute workflow-like processes, simply, scalably, and reliably.

  Examples of applications that could be powered by processes defined and executed with Journey:
  * a food delivery application,
  * a web site for computing horoscopes,
  * a web site for accepting and processing credit card applications.

  Journey process executions are designed to be persistent and resilient, to survivce service restarts, and to, quite literally, scale with your service.

  ## Project Status

  Here is the project's current state and mini-roadmap:

  - [x] Initial version, the state of executions lives in memory, does not survive service restarts.
  - [ ] The state of executions is persisted, executions survive service restarts.
  - [ ] Executions run in multiple replicas of your service.
  - [ ] Maybe: loops in steps.
  - [ ] Maybe: support for specific persistence types.
  - [ ] Maybe: support for specific persistence types.
  - [ ] Maybe: timer-based cron-like executions.
  - [ ] Maybe: better naming / metaphores for Journey's / Processes / Executions
  - [ ] Maybe: Documentation includes an example application.
  - [ ] Maybe: Documentation includes examples of versioned processes.
  - [ ] Maybe: Retry policy is configurable, clearly documented.
  - [ ] Maybe: Logging is configurable, clearly documented.
  - [ ] Maybe: Monitoring is clearly documented.
  - [ ] Maybe: Performance and scalability are clearly documented.
  - [ ] Maybe: More concise and expressive ways to define journies.

  The project is in active development. For questions, comments, bug reports, feature requests please create issues (and/or Pull Requests:).


  ## Installation

  The package can be installed from Hex by adding `journey` to your list of dependencies in `mix.exs`:

  ```elixir
  def deps do
  [
    {:journey, "~> 0.0.1"}
  ]
  end
  ```

  ## Example: a Web Site for Computing Horoscopes

  Imagine a web site that computes horoscopes.

  To power this web site we will define a `Journey.Process`, consisting of a collection of `Journey.Step`s.

  Some of the steps (`:user_name`, `:birth_day`, `:birth_month`) get their values from the user, while others (`:astrological_sign`, `:horoscope`) compute their values based on the data captured or computed so far, using `func`tions that are part of those steps' definitions.

  This code fragment, defines the process, and then executes it, step by step, from the user (Mario) entering their name and birthday, to the process coming back with Mario's horoscope.

  ```elixir
  iex> process = %Journey.Process{
  ...>  name: "horoscopes-r-us",
  ...>  version: "0.0.1",
  ...>  steps: [
  ...>    %Journey.Step{name: :first_name},
  ...>    %Journey.Step{name: :birth_month},
  ...>    %Journey.Step{name: :birth_day},
  ...>    %Journey.Step{
  ...>      name: :astrological_sign,
  ...>      func: fn _values ->
  ...>        # Everyone is a Taurus!
  ...>        {:ok, :taurus}
  ...>      end,
  ...>      blocked_by: [
  ...>        birth_month: :provided,
  ...>        birth_day: :provided
  ...>      ]
  ...>    },
  ...>    %Journey.Step{
  ...>      name: :horoscope,
  ...>      func: fn values ->
  ...>        name = values[:first_name].value
  ...>        sign = Atom.to_string(values[:astrological_sign].value)
  ...>        {
  ...>          :ok,
  ...>          "#{name}! You are a righteous #{sign}! This is the perfect week to smash the racist patriarchy!"
  ...>        }
  ...>      end,
  ...>      blocked_by: [
  ...>        first_name: :provided,
  ...>        astrological_sign: :provided
  ...>      ]
  ...>    }
  ...>  ]
  ...>}
  iex>
  iex> # Start an execution of the process.
  iex> # (this could be called by the app's phoenix controller, when a user starts the process on the web site).
  iex> execution = Journey.Process.execute(process)
  iex>
  iex> # The user entered their name. Update the execution.
  iex> # (this could be called by the app's Phoenix controller, when a user submits their name).
  iex> {:ok, execution} = Journey.Execution.update_value(execution, :first_name, "Mario")
  iex>
  iex> # Mario entered their birth month and day. Update the execution.
  iex> # (this could be called by the app's Phoenix controller, when a user submits these values).
  iex> {:ok, execution} = Journey.Execution.update_value(execution, :birth_month, 3)
  iex> {:ok, execution} = Journey.Execution.update_value(execution, :birth_day, 10)
  iex>
  iex> # :astrological_sign is no longer blocked, and it is now ":computing".
  iex> {:computing, _} = Journey.Execution.read_value(execution, :astrological_sign)
  iex>
  iex> # Get a human friendly textual summary of the current status of this execution, in case we want to take a look.
  iex> execution.execution_id |> Journey.Execution.load!() |> Journey.Execution.get_summary() |> IO.puts()
  iex>
  iex> # Get all values in this execution, in case the code wants to take a look.
  iex> values = Journey.Execution.get_all_values(execution)
  iex> values[:first_name][:status]
  :computed
  iex> values[:first_name][:value]
  "Mario"
  iex>
  iex> # In just a few milliseconds, we will have Mario's astrological sign! Well, kinna.
  iex> :timer.sleep(100)
  iex> {:computed, :taurus} = execution.execution_id |> Journey.Execution.load!() |> Journey.Execution.read_value(:astrological_sign)
  iex>
  iex> # :horoscope computation is no longer blocked. In just a few milliseconds, we will have Mario's horoscope.
  iex> # The web page or the app's Phoenix controller can poll for this value, and render it when it becomes :computed.
  iex> :timer.sleep(100)
  iex> {:computed, horoscope} = execution.execution_id |> Journey.Execution.load!() |> Journey.Execution.read_value(:horoscope)
  iex> horoscope
  "Mario! You are a righteous taurus! This is the perfect week to smash the racist patriarchy!"
  ```

  `Journey.Execution` execution will save every value it receives or computes, so, even if the server dies (TODO: link to the issue), the execution will continue where it left off, whenever there is a replica of your application running "horoscopes-r-us" process (unless you are using one-node-in-memory configuration). And, because `Journey.Process` runs as part of your application, it, quite literally, scales with your application.

  In our example, `func`tions are very simple, but if your function is, for some reason, temporarily unable to compute the value, it can return `{:retriable, error_information}`, and Journey will retry it, according to the step's (implicit, in our example) retry policy. (TODO: link to the issue for implementing this).

  # Introspection

  At any point in the lifetime of an execution, you can get a human-friendly summary of its state:

  ```elixir
  execution.execution_id |> Journey.Execution.load!() |> Journey.Execution.get_summary |> IO.puts
  ```
  ```text
  Execution Summary
  Execution ID: hs5ijpaif7
  Execution started: 2021-03-13 07:59:08Z
  Revision: 2
  All Steps:
  [started_at]: '1615622348'. Blocked by: []. Self-computing: false
  [first_name]: 'not_computed'. Blocked by: []. Self-computing: false
  [birth_month]: 'not_computed'. Blocked by: []. Self-computing: false
  [birth_day]: '29'. Blocked by: []. Self-computing: false
  [astrological_sign]: 'not_computed'. Blocked by: [birth_month]. Self-computing: true
  [horoscope]: 'not_computed'. Blocked by: [first_name, astrological_sign]. Self-computing: true

  :ok
  ```

  ## Logging

  TODO: document.

  ## Monitoring

  TODO: implement, document.

  ## Documentation

  Full documentation can be found at [https://hexdocs.pm/journey](https://hexdocs.pm/journey).
  """
end
