# ALWAYS

Please strive to provide factual data based on up-to-date reputable sources (e.g. hexdocs.pm). When making an assumption or making a guess, please say so. Facts and non-facts need to be clearly differentiated.

# CLAUDE.md

*This file provides project context to Claude Code for effective AI-assisted development.*

## Journey: Computation Graph Library

Journey is an Elixir library for building persistent, scalable computation graphs. It manages data flows, executions, and background scheduling with PostgreSQL persistence.

## Development Workflow

### Essential Commands
- `make validate` - **Run this before declaring any change complete**
- `make test` - Full test suite with coverage
- `mix test path/to/test.exs` - Single test file
- `make test-performance` - Performance benchmarks 
- `elixir -e "IO.puts(\"Hello from the command line\")"`

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

## Core Architecture

### Documentation

The generated documentation for this package lives under `./doc`, and can be rebuild with `make build-docs`:
```
$ make build-docs
mix docs --proglang elixir
Generated journey app
Generating docs...
View "html" docs at "doc/index.html"
View "epub" docs at "doc/Journey.epub"
```

Please read the documentation to understand what the package is expected to do. Update documentation as needed.


### Graph Components
```elixir
# Nodes created with Journey.Node functions:
input/1           # User-provided data
compute/4         # Self-computing with dependencies  
mutate/4          # Modifies other node values
schedule_once/3   # One-time scheduled execution
schedule_recurring/3  # Recurring execution
```

### Key APIs
```elixir
Journey.new_graph/3      # Define computation graph
Journey.start_execution/1 # Execute graph instance
Journey.load/2           # Load existing execution
Journey.set_value/3      # Set node values
Journey.get_value/3      # Retrieve node values
```

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

- **Credo linting** in strict mode
- **Format enforcement** via `mix format`
- **Zero warnings** - all treated as errors
- **Security review** required for new dependencies

---

*When contributing: Ask clarifying questions, write simple tests, validate with `make validate`, and prioritize code clarity for Elixir developers.*
