# ALWAYS

Please strive to provide factual data based on up-to-date reputable sources (e.g. hexdocs.pm). When making an assumption or making a guess, please say so. Facts and non-facts need to be clearly differentiated.

Choose simplicity, clarity and readability over cleverness.

Read the summary of modules and functions in ./MODULES_AND_FUNCTIONS.md

## Journey: Durable Workflows, as a Package

Journey is an Elixir library (published on hex: https://hexdocs.pm/journey) for defining and running durable workflows as persistent reactive graphs, with PostgreSQL persistence, retries, crash recovery, horizontal scalability, scheduling, introspection and analytics. It is a package, not a service: an application adds `:journey` to its deps, configures `Journey.Repo` to point at a Postgres database, and registers its graphs via `config :journey, :graphs`.

## Development Workflow

### Essential Commands
- `make validate` - format-check + build (warnings-as-errors) + lint + full tests. **Run this before declaring any change complete**
- `make test` - drops/recreates/migrates the test DB, then runs the full suite with coverage (threshold set in `mix.exs`)
- `mix test path/to/test.exs` - single test file; `mix test path/to/test.exs:42` - single test
- `make lint` - Credo in strict mode + `mix hex.outdated` + `mix hex.audit`
- `make format` / `make format-check`
- `make test-performance` - performance benchmarks (`test_load/performance_benchmark.exs`, runs against the dev DB)
- `make build-docs` - regenerate package docs into `./doc`
- `make db-local-rebuild` - (re)create the local Postgres Docker container (`new_journey-postgres-db`)

Running elixir code from CLI:
- `elixir -e "IO.puts(\"Hello from the command line\")"`
- `mix run -e "IO.puts \"Hello from Elixir\""`

Please see Makefile for other useful commands and shortcuts.

Note that `make` commands return non-zero exit codes on failure. A successful run (exit code 0) means everything passed — do not re-run to scan for errors in the output.

### Checklist for making changes

Before declaring a change "done", ask yourself the following questions:
- did I run `make validate`, and fix all issues or raised concerns?
- did I run `make test-performance` a few times, and assessed the performance impact of the change?
- can this functionality be implemented in a simpler fashion, or does this change strike a good balance of simplicity and functionality?

### Quality Standards
- **Idiomatic Elixir**: Favor Elixir patterns over concepts from other languages
- **Minimal changes**: Simple, focused modifications with basic tests
- **Security-first**: Protect customer data and prevent vulnerabilities
- **Coverage**: Update `mix.exs` threshold if coverage increases
- **Readability**: Favor simplicity, clarity and readability over cleverness. Prefer explicit anonymous functions over capture operators.
- **Zero warnings**: builds use `--warnings-as-errors`

## Core Architecture

### The Data Model

An execution's entire state lives in Postgres (schemas in `lib/journey/persistence/schema/`):
- `executions` — one row per graph execution, carrying a monotonically increasing `revision`
- `values` — one row per node, recording the value and the execution revision (`ex_revision`) at which it was set
- `computations` — one row per computation attempt, with state (`:not_set`, `:computing`, `:success`, `:failed`, ...) and the revisions of the dependencies it consumed

This revision bookkeeping is how Journey decides what is unblocked and what needs recomputation — there is no in-memory state to lose; any replica can pick up any execution.

### The Reactive Loop

The core flow (starting in `lib/journey/scheduler.ex`):
1. `Journey.set/3` persists a value, bumps the execution revision, and calls `Journey.Scheduler.advance/1`.
2. `advance/1` migrates the execution to the current graph version if needed (`Journey.Executions.GraphSchemaEvolution`), detects upstream changes and creates re-computations (`Journey.Scheduler.Recompute`), then atomically claims unblocked computations (`Journey.Scheduler.Available`, DB-locked so concurrent replicas don't double-run).
3. Each claimed computation runs as a fire-and-forget Task; `Journey.Scheduler.Completions` records the result (scheduling retries on failure, see `Journey.Scheduler.Retry`) and calls `advance/1` again. The cycle repeats until nothing is unblocked.

Background sweeps (`Journey.Scheduler.Background.Periodic`, default every 60s, configurable via `config :journey, :background_sweeper`) are the durability net that makes computations survive crashes and restarts: `lib/journey/scheduler/background/sweeps/` contains sweeps for abandoned computations, schedule nodes (`tick_once`/`tick_recurring`), missed schedules, and stalled executions.

Graphs themselves are not persisted as data: they are registered at application start into `Journey.Graph.Catalog` (an in-memory registry) from the `config :journey, :graphs` list of factory functions. `priv/repo/migrations` holds Journey's own migrations, run automatically at app start via `Ecto.Migrator`.

### Graph Components
```elixir
# Nodes created with Journey.Node functions:
input/2           # User-provided data
compute/4         # Self-computing with dependencies
mutate/4          # Modifies other node values
historian/3       # Chronological log of node changes
archive/3         # Archives execution when unblocked
tick_once/4       # One-time scheduled execution
tick_recurring/4  # Recurring execution
loop/4            # Iterative compute (requires :max_iterations)
```

Conditional dependencies are expressed with `Journey.Node.UpstreamDependencies.unblocked_when/1` predicate trees — `:and`/`:or` nest recursively; `:not` applies only to a single `{node, condition}` leaf — over `Journey.Node.Conditions` helpers (`provided?/1`, `true?/1`, `false?/1`). `unblocked_when/2` is a convenience form for a single `(node, condition)` pair.

### Key APIs
```elixir
Journey.new_graph/4      # Define computation graph
Journey.start/1           # Execute graph instance
Journey.load/2           # Load existing execution
Journey.set/3            # Set node values
Journey.unset/2          # Unset node values (single or multiple)
Journey.get/3            # Retrieve node values (preferred over get_value)
Journey.values/2         # Map of all set node values
Journey.Tools.introspect/1  # Primary debugging tool for an execution's state
```

Please read lib/journey.ex and lib/journey/node.ex for Journey's API functions, their documentation and usage examples.
Please read MODULES_AND_FUNCTIONS.md for a high-level description of modules and functions provided by Journey.

### Module Organization
- `Journey` (lib/journey.ex) - Main API
- `Journey.Node` (lib/journey/node.ex, lib/journey/node/) - Node types and unblock conditions
- `Journey.Graph` (lib/journey/graph/) - Graph definition, validation (incl. cycle detection), Catalog registry
- `Journey.Scheduler` (lib/journey/scheduler/) - advance loop, claiming, retries, background sweeps
- `Journey.Executions` (lib/journey/executions/) - Persistence layer, list/count query building, graph schema evolution
- `Journey.Persistence.Schema.Execution` - Ecto schemas (Execution, Value, Computation)
- `Journey.Tools` - Debugging and introspection
- `Journey.Insights` - FlowAnalytics (business analytics) and Status (system health)

### Documentation

The generated documentation for this package lives under `./doc`, and can be rebuilt with `make build-docs`.

Please read the documentation to understand what the package is expected to do. Update documentation as needed. `README.md` and `BASIC_CONCEPTS.md` are part of the published docs (see `mix.exs` extras), as are the livebooks in `lib/examples/`.

Make sure that the list of public modules and functions in MODULES_AND_FUNCTIONS.md continues to be accurate.

Only functions that are surfaced at the API level need to be documented. Internal functions and modules can still have documentation, but as `# ...` blocks – not surfaced to package documentation.

## Database & Testing

**PostgreSQL Setup**: Development and test databases run in Docker containers
```bash
# Access development DB (for performance tests)
docker exec -it new_journey-postgres-db psql -U postgres journey_dev

# Access test DB (for make test)
docker exec -it new_journey-postgres-db psql -U postgres journey_test
```

**Testing Patterns**:
- ExUnit with doctests in module documentation (see `test/journey/doc_test.exs`)
- Use `redact/2` helper for masking dynamic values (IDs, timestamps)
- Background sweeps in tests: `Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test/1` (and the matching `stop_background_sweeps_in_test/1`)
- Performance tests in `test_load/performance_benchmark.exs`; load test in `test_load/sunny_day.exs`

---

*When contributing: Ask clarifying questions, write simple tests, validate with `make validate`, and prioritize code clarity for Elixir developers.*
