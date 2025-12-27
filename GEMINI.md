# Journey Context for Gemini

## Project Overview

**Journey** is an Elixir library designed for building persistent, scalable, and durable workflow graphs. It allows developers to define complex data flows (steps, dependencies, computations) that are resilient to system restarts and crashes. It uses PostgreSQL for state persistence and orchestration.

*   **Core Concept:** "Durable Workflows as a Package".
*   **Primary Language:** Elixir (~> 1.18).
*   **Persistence:** PostgreSQL via Ecto.
*   **Distribution:** Published on Hex.pm.

## Key Technologies

*   **Elixir:** The core language.
*   **Ecto:** Database wrapper and query generator.
*   **PostgreSQL:** The underlying database engine (running in Docker for dev/test).
*   **Docker:** Used to host the local PostgreSQL instance.
*   **Mix:** The build tool.

## Development Workflow & Commands

The project relies heavily on `make` to orchestrate common tasks.

### Critical Commands

*   `make validate`: **ALWAYS run this before considering a task complete.** It runs formatting checks, linting (Credo), and the full test suite.
*   `make test`: Runs the test suite with coverage.
*   `make test-performance`: Runs performance benchmarks (`test_load/performance_benchmark.exs`).
*   `make format`: Formats the code.
*   `make lint`: Runs strict Credo checks.

### Other Useful Commands

*   `mix test path/to/test.exs`: Run a specific test file.
*   `mix docs`: Generate HTML documentation in `doc/`.
*   `mix ecto.create`: Create the database (usually handled by `make` targets).

## Architecture & API

### Core Modules

*   `Journey` (`lib/journey.ex`): The main public API for interacting with executions (start, set, get, load).
*   `Journey.Node` (`lib/journey/node.ex`): Helper functions for defining graph nodes (`input`, `compute`, `mutate`, `tick_once`, `tick_recurring`).
*   `Journey.Graph`: Logic for graph definition and validation.
*   `Journey.Repo`: Ecto repository configuration.
*   `Journey.Scheduler`: Handles background processing and job execution.

### Key Concepts

1.  **Graph:** A static definition of a workflow, consisting of nodes and dependencies. Defined via `Journey.new_graph/3`.
2.  **Execution:** A running instance of a Graph. Started via `Journey.start_execution/1`.
3.  **Nodes:**
    *   `input`: Data provided from outside.
    *   `compute`: Data derived from other nodes via a function.
    *   `mutate`: Modifies the value of another node.
    *   `tick_*`: Time-based triggers.
4.  **Persistence:** All state transitions (setting a value, computing a result) are immediately persisted to PostgreSQL.

## Testing Standards

*   **Zero Warnings:** The project compiles with `--warnings-as-errors`.
*   **Doctests:** Extensive use of doctests in module documentation.
*   **Performance:** Performance is a key metric; changes should be verified against `test_load/performance_benchmark.exs` if they impact core execution logic.
*   **Strict Linting:** Credo is configured in strict mode.

## Code Style & Conventions

*   **Idiomatic Elixir:** Follow standard Elixir community practices.
*   **Readability:** Prefer clarity over cleverness.
*   **Explicit Functions:** Prefer explicit anonymous functions `fn x -> ... end` over capture syntax `&...` for complex logic.
*   **Redaction:** Use `redact/2` helpers in tests to mask dynamic values like UUIDs or timestamps when asserting on structs.

## Database Access

*   Development DB: `new_journey-postgres-db` (Docker container).
*   Test DB: Separate database for running `mix test`.
*   CLI Access: `make db-local-psql` connects to the local postgres instance.
