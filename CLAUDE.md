# ALWAYS

Please strive to provide factual data based on up-to-date reputable sources (e.g. hexdocs.pm). When making an assumption or making a guess, please say so. Facts and non-facts need to be clearly differentiated.

Choose simplicity, clarity and readability over cleverness.

Read the summary of modules and functions in ./MODULES_AND_FUNCTIONS.md

# CLAUDE.md

*This file provides project context to Claude Code for effective AI-assisted development.*

## Journey: Computation Graph Library

Journey is an Elixir library for building persistent and scalable reactive graphs. It manages data flows, executions, and background scheduling with PostgreSQL persistence.

The library is published on hex: https://hexdocs.pm/journey


## Development Workflow

### Essential Commands
- `make validate` - **Run this before declaring any change complete**
- `make test` - Full test suite with coverage
- `mix test path/to/test.exs` - Single test file
- `make test-performance` - Performance benchmarks 
Running elixir code from CLI:
- `elixir -e "IO.puts(\"Hello from the command line\")"`
or
- 
```
~/src/new_journey $ mix run -e "IO.puts \"Hello from Elixir\""
Compiling 1 file (.ex)
Generated journey app
Hello from Elixir
```

Please see Makefile for other useful commands and shortcuts.

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

## Core Architecture

### Documentation

The generated documentation for this package lives under `./doc`, and can be rebuilt with `make build-docs`:
```
$ make build-docs
mix docs --proglang elixir
Generated journey app
Generating docs...
View "html" docs at "doc/index.html"
View "epub" docs at "doc/Journey.epub"
```

Please read the documentation to understand what the package is expected to do. Update documentation as needed.

Make sure that the list of public modules and functions in MODULES_AND_FUNCTIONS.md continues to be accurate.


### Graph Components
```elixir
# Nodes created with Journey.Node functions:
input/1           # User-provided data
compute/4         # Self-computing with dependencies
mutate/4          # Modifies other node values
tick_once/3       # One-time scheduled execution
tick_recurring/3  # Recurring execution
```

### Key APIs
```elixir
Journey.new_graph/3      # Define computation graph
Journey.start_execution/1 # Execute graph instance
Journey.load/2           # Load existing execution
Journey.set/3            # Set node values
Journey.unset/2          # Unset node values (single or multiple)
Journey.get_value/3      # Retrieve node values
```

Please read lib/journey.ex and lib/journey/node.ex for Journey's API functions, their documentation and usage examples.
Please read MODULES_AND_FUNCTIONS.md for a high-level description of modules and functions provided by Journey.

### Module Organization
- `Journey` - Main API
- `Journey.Graph` - Graph definition/validation
- `Journey.Persistence.Schema.Execution` - State management (Ecto schema)
- `Journey.Scheduler` - Background processing
- `Journey.Executions` - Persistence layer

## Database & Testing

**PostgreSQL Setup**: Development and test databases run in Docker containers
```bash
# Access development DB (for performance tests)
docker exec -it new_journey-postgres-db psql -U postgres journey_dev

# Access test DB (for make test)  
docker exec -it new_journey-postgres-db psql -U postgres journey_test
```

**Testing Patterns**:
- ExUnit with doctests in module documentation
- Use `redact/2` helper for masking dynamic values (IDs, timestamps)
- Background sweeps: `Journey.Scheduler.Background.Periodic.start_background_sweeps_in_test/1`
- Performance tests in `test_load/performance_benchmark.exs`

## Code Quality

- **Credo linting** in strict mode (`make lint`)
- **Format enforcement** via `make format`
- **Zero warnings** - all treated as errors
- **Security review** required for new dependencies

---

*When contributing: Ask clarifying questions, write simple tests, validate with `make validate`, and prioritize code clarity for Elixir developers.*
