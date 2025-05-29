# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Testing
- `mix test` - Run all tests with coverage and warnings as errors
- `mix test test/path/to/specific_test.exs` - Run a specific test file
- `mix test.watch` - Watch for file changes and run tests automatically

### Building and Quality Checks
- `make all` - Run full build pipeline: build, format-check, lint, test
- `make build` - Clean, compile with warnings as errors, generate docs
- `make format` - Format code with `mix format`
- `make format-check` - Check if code is formatted (CI-friendly)
- `make lint` - Run Credo linting with strict mode
- `make test` - Run tests with coverage

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