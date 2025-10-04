# Metadata Support for Journey Input Nodes

## Overview

Add optional metadata support to Journey's `set()` and `get()` operations, enabling audit trails and context tracking (e.g., author_id, timestamp, IP address) for input node changes.

## Motivation

Users need to track contextual information about who/when/why a value was set for audit logging and history tracking purposes. For example, in a reactive todo item graph, tracking which user made changes to title, description, or due_date fields.

## Design Principles

1. **Metadata is attached only via `set()` on input nodes** - computed nodes don't store their own metadata, they only read upstream metadata
2. **Metadata flows downstream** - compute functions can access upstream node metadata via optional `/2` arity
3. **Breaking change** - this is a major version bump due to `get()` return format change
4. **Backward compatible compute functions** - existing `/1` f_compute functions continue to work

## Database Schema Changes

### Migration

File: `priv/repo/migrations/YYYYMMDDHHMMSS_add_metadata_to_values.exs`

```elixir
defmodule Journey.Repo.Migrations.AddMetadataToValues do
  use Ecto.Migration

  def change do
    alter table(:values) do
      add(:metadata, :jsonb)
    end
  end
end
```

### Schema Update

File: `lib/journey/persistence/schema/execution/value.ex`

Add field to schema:

```elixir
schema "values" do
  belongs_to(:execution, Journey.Persistence.Schema.Execution)
  field(:node_name, :string)
  field(:node_type, Ecto.Enum, values: [:input | ComputationType.values()])
  field(:node_value, Journey.Persistence.Schema.Execution.Value.JsonbScalar, default: nil)
  field(:metadata, :map, default: nil)  # <-- NEW
  field(:set_time, :integer, default: nil)
  field(:ex_revision, :integer, default: nil)
  timestamps()
end
```

## API Changes

### Journey.set() - Accept Optional Metadata

Update all `set()` function clauses in `lib/journey.ex` to accept optional `metadata:` parameter:

**Signature variations:**
```elixir
# Single value
Journey.set(execution, :node_name, value, metadata: %{author_id: "user123"})
Journey.set(execution_id, :node_name, value, metadata: %{author_id: "user123"})

# Multiple values (bulk set)
Journey.set(execution, %{title: "New", description: "Updated"}, metadata: %{author_id: "user123"})
Journey.set(execution, [title: "New", description: "Updated"], metadata: %{author_id: "user123"})
```

**Implementation notes:**
- Add `opts` parameter to single-value `set()` functions, extract `metadata` from opts
- Add `opts` parameter to bulk `set()` functions - metadata applies to ALL values set in that call
- Default metadata to `nil` when not provided
- Pass metadata down to `Executions.set_value()` and `Executions.set_values()`

### Journey.get() - Return Metadata (BREAKING)

**Old signature:**
```elixir
{:ok, value, revision} = Journey.get(execution, :node_name)
{:error, :not_set}
{:error, :computation_failed}
```

**New signature:**
```elixir
{:ok, {value, metadata, revision}} = Journey.get(execution, :node_name)
{:error, :not_set}
{:error, :computation_failed}
```

**Migration pattern for users:**
```elixir
# Before:
{:ok, value, revision} = Journey.get(execution, :node)

# After:
{:ok, {value, metadata, revision}} = Journey.get(execution, :node)

# Or ignore metadata:
{:ok, {value, _metadata, revision}} = Journey.get(execution, :node)
```

## Persistence Layer Changes

### Executions.set_value()

File: `lib/journey/executions.ex`

Update both `set_value/3` (by execution_id) and `set_value/3` (by execution) clauses:

1. Add `metadata` parameter (default `nil`)
2. Include metadata in the `repo.update_all()` set clause:

```elixir
|> repo.update_all(
  set: [
    ex_revision: new_revision,
    node_value: value,
    metadata: metadata,  # <-- NEW
    updated_at: now_seconds,
    set_time: now_seconds
  ]
)
```

### Executions.set_values()

File: `lib/journey/executions.ex`

Update `set_values/2`:

1. Add `metadata` parameter (applies to all values in the batch)
2. Update `update_value_in_transaction/6` to accept and persist metadata
3. Include metadata in all `repo.update_all()` calls

### Executions.get_value() and get_value_node()

File: `lib/journey/executions.ex`

Update return handling:

**Currently:**
```elixir
def get_value(execution, node_name, timeout_ms, opts) do
  case get_value_node(execution, node_name, timeout_ms, opts) do
    {:ok, value_node} -> {:ok, value_node.node_value}
    error -> error
  end
end
```

**Change to:**
```elixir
def get_value(execution, node_name, timeout_ms, opts) do
  case get_value_node(execution, node_name, timeout_ms, opts) do
    {:ok, value_node} ->
      {:ok, {value_node.node_value, value_node.metadata, value_node.ex_revision}}
    error -> error
  end
end
```

### Journey.get() wrapper

File: `lib/journey.ex`

Update the wrapper in `Journey.get/3`:

```elixir
result = Executions.get_value_node(execution, node_name, timeout_ms_or_infinity, internal_opts)

case result do
  {:ok, value_node} -> {:ok, {value_node.node_value, value_node.metadata, value_node.ex_revision}}
  error -> error
end
```

## Scheduler Changes - Support f_compute/2

### Introspect Function Arity

File: `lib/journey/scheduler.ex` in `launch_computation/3`

**Current code:**
```elixir
r =
  try do
    graph_node.f_compute.(computation_params)
  rescue
    # ...
  end
```

**New code:**
```elixir
r =
  try do
    # Build metadata map from upstream nodes
    metadata_map = build_metadata_map(execution, conditions_fulfilled)

    # Introspect arity and call accordingly
    case Function.info(graph_node.f_compute)[:arity] do
      1 ->
        graph_node.f_compute.(computation_params)

      2 ->
        graph_node.f_compute.(computation_params, metadata_map)

      arity ->
        raise ArgumentError,
          "f_compute must be arity 1 or 2, got arity #{arity} for node #{computation.node_name}"
    end
  rescue
    # ...
  end
```

### Build Metadata Map

Add helper function in `lib/journey/scheduler.ex`:

```elixir
defp build_metadata_map(execution, conditions_fulfilled) do
  conditions_fulfilled
  |> Enum.map(fn %{upstream_node: value_node} ->
    {value_node.node_name, value_node.metadata}
  end)
  |> Enum.into(%{})
end
```

**Note:** `conditions_fulfilled` contains `%{upstream_node: value_node}` structs where `value_node` is an `Execution.Value` that already has the metadata field.

## Historian Enhancement

### Update build_historian_function to use /2 arity

File: `lib/journey/node.ex`

**Current signature:**
```elixir
defp build_historian_function(history_node_name, tracked_field, max_entries) do
  fn inputs ->
    # ...
    {:ok, final_history}
  end
end
```

**New signature:**
```elixir
defp build_historian_function(history_node_name, tracked_field, max_entries) do
  fn inputs, metadata_map ->
    existing_history = Map.get(inputs, history_node_name, [])

    new_entry =
      if Map.has_key?(inputs, tracked_field) do
        %{
          "value" => Map.get(inputs, tracked_field),
          "node" => to_string(tracked_field),
          "timestamp" => System.system_time(:second),
          "metadata" => Map.get(metadata_map, tracked_field)  # <-- NEW
        }
      else
        nil
      end

    # ... rest of logic
    {:ok, final_history}
  end
end
```

## Documentation Updates

### Journey.set()

Update `@doc` in `lib/journey.ex` to document the `metadata:` option:

```elixir
@doc """
...

## Options
* `:metadata` - Optional map of metadata to attach to the value(s).
  Useful for audit trails (e.g., `%{author_id: "user123", ip: "192.168.1.1"}`).
  For bulk operations, the same metadata is applied to all values.

## Examples

Set a value with metadata:
```elixir
execution = Journey.set(execution, :title, "New Title", metadata: %{author_id: "user123"})
```

Bulk set with metadata:
```elixir
execution = Journey.set(execution, %{
  title: "New Title",
  description: "Updated description"
}, metadata: %{author_id: "user123", session_id: "abc"})
```
"""
```

### Journey.get()

Update `@doc` in `lib/journey.ex`:

```elixir
@doc """
...

## Returns
* `{:ok, {value, metadata, revision}}` – the value is set, with its metadata and revision number
* `{:error, :not_set}` – the value is not yet set
* `{:error, :computation_failed}` – the computation permanently failed

## Examples
```elixir
# Get value with metadata
{:ok, {value, metadata, revision}} = Journey.get(execution, :title)

# Ignore metadata if not needed
{:ok, {value, _metadata, revision}} = Journey.get(execution, :title)
```
"""
```

### Journey.Node.compute()

Update `@doc` in `lib/journey/node.ex`:

```elixir
@doc """
...

`f_compute` is the function that computes the value of the node, once the upstream dependencies are satisfied.
The function can have arity 1 or 2:

**Arity 1** (existing behavior):
```elixir
fn %{upstream_node: value} -> {:ok, result} end
```

**Arity 2** (new, for accessing metadata):
```elixir
fn values_map, metadata_map ->
  author = get_in(metadata_map, [:title, "author_id"])
  {:ok, result}
end
```

The `metadata_map` contains metadata for all upstream nodes, keyed by node name (as atoms).
For example: `%{title: %{"author_id" => "user123"}, description: %{"author_id" => "user456"}}`

The function returns a tuple: `{:ok, value}` or `{:error, reason}`.
...
"""
```

### Journey.Node.historian()

Update `@doc` in `lib/journey/node.ex`:

```elixir
@doc """
...

History entries now include metadata (if available):
```elixir
[
  %{
    "value" => "First version",
    "node" => "content",
    "timestamp" => 1234567890,
    "metadata" => %{"author_id" => "user123"}
  },
  %{
    "value" => "Second version",
    "node" => "content",
    "timestamp" => 1234567895,
    "metadata" => %{"author_id" => "user456"}
  }
]
```
...
"""
```

## Testing Strategy

### Update Existing Tests (~300 assertions)

All tests using `Journey.get()` must be updated to handle the new return format.

**Search pattern:**
```bash
rg '\{:ok.*=.*Journey\.get' --files-with-matches
```

**Common patterns to update:**

```elixir
# Old:
{:ok, value, revision} = Journey.get(execution, :node)
assert value == "expected"

# New:
{:ok, {value, _metadata, revision}} = Journey.get(execution, :node)
assert value == "expected"
```

```elixir
# Old:
assert {:ok, "result", _} = Journey.get(execution, :node, wait: :any)

# New:
assert {:ok, {"result", _metadata, _revision}} = Journey.get(execution, :node, wait: :any)
```

### New Test Files

#### test/journey/metadata_test.exs

Test basic metadata functionality:

```elixir
defmodule Journey.MetadataTest do
  use ExUnit.Case, async: true
  import Journey.Node

  test "set and get value with metadata" do
    graph = Journey.new_graph("metadata test", "v1.0.0", [
      input(:title)
    ])

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :title, "Hello", metadata: %{author_id: "user123"})

    {:ok, {value, metadata, _revision}} = Journey.get(execution, :title)
    assert value == "Hello"
    assert metadata == %{"author_id" => "user123"}
  end

  test "set without metadata returns nil metadata" do
    graph = Journey.new_graph("no metadata test", "v1.0.0", [input(:title)])
    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :title, "Hello")

    {:ok, {_value, metadata, _revision}} = Journey.get(execution, :title)
    assert metadata == nil
  end

  test "bulk set with metadata applies to all values" do
    graph = Journey.new_graph("bulk metadata", "v1.0.0", [
      input(:title),
      input(:description)
    ])

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, %{
      title: "Title",
      description: "Desc"
    }, metadata: %{author_id: "user123"})

    {:ok, {_, title_meta, _}} = Journey.get(execution, :title)
    {:ok, {_, desc_meta, _}} = Journey.get(execution, :description)

    assert title_meta == %{"author_id" => "user123"}
    assert desc_meta == %{"author_id" => "user123"}
  end
end
```

#### test/journey/metadata_compute_test.exs

Test f_compute/2 receiving metadata:

```elixir
defmodule Journey.MetadataComputeTest do
  use ExUnit.Case, async: true
  import Journey.Node

  test "f_compute/2 receives metadata from upstream nodes" do
    graph = Journey.new_graph("compute with metadata", "v1.0.0", [
      input(:title),
      compute(
        :title_with_author,
        [:title],
        fn %{title: title}, metadata_map ->
          author = get_in(metadata_map, [:title, "author_id"]) || "unknown"
          {:ok, "#{title} by #{author}"}
        end
      )
    ])

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :title, "Hello", metadata: %{author_id: "user123"})

    {:ok, {result, _, _}} = Journey.get(execution, :title_with_author, wait: :any)
    assert result == "Hello by user123"
  end

  test "f_compute/1 still works without metadata" do
    graph = Journey.new_graph("compute without metadata", "v1.0.0", [
      input(:name),
      compute(:greeting, [:name], fn %{name: name} -> {:ok, "Hello #{name}"} end)
    ])

    execution = Journey.start_execution(graph)
    execution = Journey.set(execution, :name, "Alice", metadata: %{author_id: "user123"})

    {:ok, {result, _, _}} = Journey.get(execution, :greeting, wait: :any)
    assert result == "Hello Alice"
  end
end
```

#### test/journey/node/historian_metadata_test.exs

Test historian including metadata:

```elixir
defmodule Journey.Node.HistorianMetadataTest do
  use ExUnit.Case, async: true
  import Journey.Node

  test "historian includes metadata in history entries" do
    graph = Journey.new_graph("historian with metadata", "v1.0.0", [
      input(:content),
      historian(:content_history, [:content])
    ])

    execution = Journey.start_execution(graph)

    # First change
    execution = Journey.set(execution, :content, "v1", metadata: %{author_id: "user123"})
    {:ok, {history1, _, _}} = Journey.get(execution, :content_history, wait: :any)

    assert length(history1) == 1
    assert hd(history1)["value"] == "v1"
    assert hd(history1)["metadata"] == %{"author_id" => "user123"}

    # Second change
    execution = Journey.set(execution, :content, "v2", metadata: %{author_id: "user456"})
    {:ok, {history2, _, _}} = Journey.get(execution, :content_history, wait: :newer)

    assert length(history2) == 2
    assert Enum.at(history2, 1)["value"] == "v2"
    assert Enum.at(history2, 1)["metadata"] == %{"author_id" => "user456"}
  end
end
```

## Migration Guide for Users

### Breaking Changes

This is a **major version bump** (e.g., v1.x → v2.0.0) due to the breaking change in `Journey.get()` return format.

### Migration Steps

1. **Update all `Journey.get()` calls:**

   ```elixir
   # Before:
   {:ok, value, revision} = Journey.get(execution, :node)

   # After:
   {:ok, {value, _metadata, revision}} = Journey.get(execution, :node)
   ```

2. **Optionally add metadata to `Journey.set()` calls:**

   ```elixir
   # Basic usage (no breaking change):
   execution = Journey.set(execution, :title, "Hello")

   # With metadata (new feature):
   execution = Journey.set(execution, :title, "Hello", metadata: %{author_id: current_user.id})
   ```

3. **Optionally upgrade compute functions to access metadata:**

   ```elixir
   # Old (still works):
   compute(:result, [:input], fn %{input: val} -> {:ok, val} end)

   # New (access metadata):
   compute(:result, [:input], fn %{input: val}, metadata ->
     author = get_in(metadata, [:input, "author_id"])
     {:ok, {val, author}}
   end)
   ```

### Rollout Recommendation

1. Run test suite after updating `get()` calls - all pattern match failures will be caught at compile time
2. Update all test files before merging
3. Update `mix.exs` version to indicate major version bump
4. Update CHANGELOG with breaking changes section
5. Consider updating test coverage threshold if coverage increases

## Implementation Checklist

- [x] Create database migration
- [x] Update Execution.Value schema
- [x] Run migration
- [ ] Update Journey.set() (4 function clauses)
- [ ] Update Executions.set_value() (2 functions)
- [ ] Update Executions.set_values()
- [ ] Update Journey.get() return format
- [ ] Update Executions.get_value() return format
- [ ] Update scheduler to introspect f_compute arity
- [ ] Add build_metadata_map helper
- [ ] Update historian to use /2 and include metadata
- [ ] Update ~300 test assertions
- [ ] Add new test files (3 files)
- [ ] Update documentation (4 modules)
- [ ] Run `make validate`
- [ ] Run `make test-performance` multiple times
- [ ] Update CHANGELOG.md
- [ ] Update mix.exs version

## Performance Considerations

- JSONB column adds minimal overhead (indexed if needed)
- Arity introspection via `Function.info/1` is fast (happens once per computation)
- Metadata map building is O(n) where n = number of upstream dependencies (typically small)

## Security Considerations

- Metadata is stored as JSONB - ensure no sensitive data is logged unintentionally
- Metadata is user-provided - validate/sanitize before storing if needed
- Consider size limits on metadata maps if abuse is a concern

## Future Enhancements

- Add index on metadata JSONB column for querying
- Support metadata queries in `Journey.list_executions()`
- Add metadata to `Journey.values()` output
- Support metadata in `Journey.unset()` operations