# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

When trying to understand the codebase, and whenever making a change, please read the file completely, and following the guidelines, and follow the "Pre-change checklist" and "Post-change checklist".


## Guidelines for Making Changes
- Be clear about the request for the change. If not clear, ask clarifying questions of the requester.
- Err on the side of minimal changes and simple tests.
- Consider creating basic tests, err on the side of simplicity, avoid mocking.
- Prioritize correctness, of course, and clarity of the code.
- Use idiomatic Elixir. Notions from other languages and ecosystems might not apply, and might be counter-productive and overly complex. Ask yourself: "is the concern I am addressing a real concern in Elixir?"
- Consider applying patterns already existing in the codebase, and incorporate existing patterns into your changes.
- When looking into using a package or an API, consider the reputation and potential vulnerability and risks involved in taking on that dependency.
- Pay attention to making sure that the system is secure and cannot be hacked into, and that customer data is secure.
- This project uses Makefile, which contains shortcuts for common operations (e.g. "test" or "validate").

## Pre-change checklist
- Plan multi-step tasks with TodoWrite
- Please only provide factual information, based on up-to-date reputable sources (e.g. package documentation on hex.pm). When making assumptions or guessing, please clearly state so.

## Post-change checklist
- Before declaring any code change completed, please validate it by running `make validate`, and address all new issues.
- If your change raised test coverage, please update test_coverage threshold in mix.exs to reflect the increase.
- Ask the question: "Can this be further simplified, or does the current implementation provide a good balance of simplicity, clarity, functionality?"

## Common Development Commands

### Validating a code or test change before declaring it done
- `make validate` - runs format checkers, linters, and tests.

### Testing
- `make test` - Run all tests with coverage and warnings as errors
- `mix test test/path/to/specific_test.exs` - Run a specific test file
- `mix test.watch` - Watch for file changes and run tests automatically

### Building and Quality Checks
- `make all` - Run full build pipeline: build, format-check, lint, test
- `make build` - Clean, compile with warnings as errors, generate docs
- `make format` - Format code with `mix format`
- `make format-check` - Check if code is formatted (CI-friendly)
- `make lint` - Run Credo linting with strict mode

### Development Database
- `make db-local-rebuild` - Recreate local PostgreSQL container
- `make db-local-psql` - Connect to local PostgreSQL via psql

### Dependencies
- `mix deps.get` - Fetch dependencies
- `make deps-get` - Same as above
- `make clean` - Clean compiled files and dependencies

## Architecture Overview

Journey is an Elixir library for building and executing computation graphs. It provides a way to define data flows and computations in applications with features like persistence, scalability, and reliability.

### Core Concepts

**Graph Definition**: Graphs are defined using `Journey.new_graph/3` with nodes that can be:
- `input/1` - User-provided data nodes
- `compute/4` - Self-computing nodes with dependencies
- `mutate/4` - Nodes that modify other node values
- `schedule_once/3` - One-time scheduled execution
- `schedule_recurring/3` - Recurring scheduled execution
- `archive/2` - Archive execution nodes

**Executions**: Graph instances are executed via `Journey.start_execution/1`. Each execution:
- Has a unique ID and revision number
- Persists state to PostgreSQL via Ecto
- Can be loaded/reloaded with `Journey.load/2`
- Values are set with `Journey.set_value/3` and retrieved with `Journey.get_value/3`

**Scheduler**: Background processing system that:
- Monitors for unblocked computations
- Handles retries and abandonment
- Manages scheduled executions
- Operates via the `Journey.Scheduler` module

### Key Modules

- `Journey` - Main API module
- `Journey.Graph` - Graph definition and validation
- `Journey.Execution` - Execution schema and state management
- `Journey.Node` - Node creation functions (input, compute, etc.)
- `Journey.Scheduler` - Background processing and task management
- `Journey.Executions` - Execution persistence and querying

### Database Setup

The application uses PostgreSQL with Ecto. Configuration in `config/` files:
- Development database runs on localhost with custom credentials
- Test database setup via `config/test.exs`
- Migrations in `priv/repo/migrations/`

### Testing Patterns

Tests use ExUnit and include:
- Doctest examples embedded in module documentation
- Integration tests for full workflow scenarios
- Background sweep simulation for scheduled nodes using `Journey.Scheduler.BackgroundSweeps.start_background_sweeps_in_test/1`
- Tests often use the `redact/2` helper to mask dynamic values like timestamps and IDs

### Development Notes

- Uses Credo for linting with strict mode enabled
- Code formatting enforced via `mix format`
- Documentation generated with ExDoc
- Test coverage threshold set to 78%
- All compilation warnings treated as errors
