# Journey.Graph.Catalog Versioning Enhancement

## Overview
The Journey.Graph.Catalog currently stores only one version of each graph, using the graph name as the sole key. This causes issues when multiple versions of a graph exist, as newer versions overwrite older ones, potentially breaking existing executions that depend on specific versions.

This specification describes enhancements to support multiple versions per graph and provide better catalog querying capabilities.

## Current Problems

1. **Version Overwriting**: When registering a graph with an existing name, it overwrites the previous version
2. **Version Mismatch**: Executions store `graph_version` but the catalog ignores it when fetching graphs
3. **No Discovery**: Cannot list available graphs or their versions
4. **Runtime Errors**: Code fetching graphs from catalog may fail if expected nodes don't exist in the current version

## Requirements

### 1. Multi-Version Storage
- The catalog must store multiple versions of the same graph simultaneously
- Use composite key `{name, version}` instead of just `name`
- Preserve all registered graph versions

### 2. New API Functions

#### `list/2` Function
Add a new `list/2` function with the following behavior:

```elixir
# List all graphs (all names, all versions)
Journey.Graph.Catalog.list()
Journey.Graph.Catalog.list(nil, nil)
# Returns: [%Journey.Graph{}, %Journey.Graph{}, ...]

# List all versions of a specific graph
Journey.Graph.Catalog.list("horoscope workflow")
Journey.Graph.Catalog.list("horoscope workflow", nil)
# Returns: [%Journey.Graph{name: "horoscope workflow", version: "v1.0.0", ...}, 
#           %Journey.Graph{name: "horoscope workflow", version: "v2.0.0", ...}]

# Get specific graph version
Journey.Graph.Catalog.list("horoscope workflow", "v1.0.0")
# Returns: [%Journey.Graph{name: "horoscope workflow", version: "v1.0.0", ...}]
# or [] if not found

# Invalid usage - raises error
Journey.Graph.Catalog.list(nil, "v1.0.0")
# Raises: ArgumentError with message "graph_version cannot be specified without graph_name"
```

#### Updated `fetch!/2` Function
Replace the current `fetch!/1` with a new `fetch!/2` that requires both parameters:

```elixir
# New signature - both parameters required
Journey.Graph.Catalog.fetch!(graph_name, graph_version)
# Returns specific version or raises if not found

# Old signature should be removed
# Journey.Graph.Catalog.fetch!(graph_name) # NO LONGER SUPPORTED
```

### 3. Internal Storage Changes
- Change Agent state from `%{name => graph}` to `%{{name, version} => graph}`

### 4. Update All Existing Code

All places that call `Journey.Graph.Catalog.fetch!(execution.graph_name)` must be updated to:
`Journey.Graph.Catalog.fetch!(execution.graph_name, execution.graph_version)`

These locations include (but may not be limited to):
- `lib/journey/graph/validations.ex`
- `lib/journey/scheduler.ex`
- `lib/journey/scheduler/helpers.ex`
- `lib/journey/tools.ex`
- Any other location using `Catalog.fetch!`

## Implementation Steps

1. **Update Catalog Storage Structure**
   - Modify the Agent's initial state in `start_link/1`
   - Update `register/1` to use `{name, version}` as key
   - The register function should still return the graph for chaining

2. **Replace `fetch!/1` with `fetch!/2`**
   - Remove the old `fetch!/1` function
   - Add new `fetch!/2` that requires both name and version
   - Raise appropriate error if graph not found

3. **Implement `list/2` Function**
   - Add argument validation (no version without name)
   - Filter results based on provided parameters
   - Return results as a list, even for single items

4. **Update All Callers**
   - Find all `Catalog.fetch!` calls
   - Update to pass both `execution.graph_name` and `execution.graph_version`
   - Ensure all callers have access to version information

5. **Add Tests**
   - Test multi-version storage
   - Test `list/2` with all parameter combinations
   - Test error cases (version without name)
   - Test that `fetch!/2` requires both parameters
   - Test that executions use correct graph versions
   - Remove any tests for single-parameter `fetch!/1`

## Testing Scenarios

1. Register multiple versions of the same graph and verify all are stored
2. List all graphs and verify complete results
3. List versions of a specific graph
4. Fetch specific versions with `fetch!/2`
5. Verify error when specifying version without name in `list/2`
6. Verify `fetch!/2` raises when graph not found
7. Create executions with different graph versions and verify they use correct versions during scheduling/computation

## Success Criteria

1. Multiple graph versions can coexist in the catalog
2. Executions always use their specific graph version
3. `list/2` provides flexible querying of available graphs
4. `fetch!/2` requires both name and version parameters
5. All tests pass, including `make validate` and `make test-performance`
6. No single-parameter `fetch!/1` function exists