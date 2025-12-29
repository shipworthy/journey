# Implementation Plan: Heartbeat & Watchdog for Journey Computations

Based on `JOURNEY_HEARTBEAT_SPEC.md`, this plan implements crash detection for long-running computations.

## Problem
Long-running jobs require large `abandon_after_seconds`. If the worker crashes early, the system won't detect it until the full timeout expires.

## Solution
Decouple crash detection (heartbeat) from max duration (deadline). Watchdog process sends periodic heartbeats; sweeper detects missing heartbeats.

---

## Phase 1: Database Migration

### 1.1 Create Migration
**File:** `priv/repo/migrations/20251229013841_add_heartbeat_fields_to_computations.exs`

```elixir
defmodule Journey.Repo.Migrations.AddHeartbeatFieldsToComputations do
  use Ecto.Migration

  def change do
    alter table(:computations) do
      add :last_heartbeat_at, :bigint, null: true
      add :heartbeat_deadline, :bigint, null: true
    end
  end
end
```

### 1.2 Update Schema
**File:** `lib/journey/persistence/schema/execution/computation.ex`

Add after `deadline` field (line ~20):
```elixir
field(:last_heartbeat_at, :integer, default: nil)
field(:heartbeat_deadline, :integer, default: nil)
```

**Design decision:** Store `heartbeat_deadline` (absolute timestamp) instead of `heartbeat_timeout_seconds`. This mirrors the existing `deadline` pattern and keeps the sweeper query simple - no graph lookups needed.

---

## Phase 2: Node Configuration

### 2.1 Update Step Struct
**File:** `lib/journey/graph/step.ex`

Add to defstruct (after `abandon_after_seconds`):
```elixir
:heartbeat_interval_seconds,
:heartbeat_timeout_seconds
```

Add to @type:
```elixir
heartbeat_interval_seconds: pos_integer(),
heartbeat_timeout_seconds: pos_integer()
```

### 2.2 Update Node Functions
**File:** `lib/journey/node.ex`

Update `compute/4`, `mutate/4`, `tick_once/4`, `tick_recurring/4`, `historian/3`, `archive/3` to extract options:
```elixir
heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240)
```

**Note:** Default interval is 70s (not 60s) to avoid coinciding with the default 60s `abandon_after_seconds`.

### 2.3 Add Validation
**File:** `lib/journey/graph/validations.ex`

Add to `validate/1` pipeline:
```elixir
|> validate_heartbeat_options()
```

Validation rule: `heartbeat_interval_seconds <= heartbeat_timeout_seconds / 2`

---

## Phase 3: Initialize Heartbeat on Computation Start

**File:** `lib/journey/scheduler/available.ex`

In `grab_this_computation/4` (lines 114-119), add to changeset:
```elixir
now = System.system_time(:second)

computation
|> Ecto.Changeset.change(%{
  state: :computing,
  start_time: now,
  ex_revision_at_start: new_revision,
  deadline: now + graph_node.abandon_after_seconds,
  last_heartbeat_at: now,
  heartbeat_deadline: now + graph_node.heartbeat_timeout_seconds
})
```

---

## Phase 4: Watchdog Implementation

### 4.1 Create Watchdog Module
**File:** `lib/journey/scheduler/watchdog.ex` (new)

```elixir
defmodule Journey.Scheduler.Watchdog do
  @moduledoc """
  Sends periodic heartbeat updates for a running computation.
  Linked to the worker process - dies when worker dies.
  """

  require Logger

  def run(computation_id, interval_seconds, timeout_seconds) do
    loop(computation_id, interval_seconds, timeout_seconds)
  end

  defp loop(computation_id, interval_seconds, timeout_seconds) do
    Process.sleep(interval_seconds * 1000)
    update_heartbeat(computation_id, timeout_seconds)
    loop(computation_id, interval_seconds, timeout_seconds)
  end

  defp update_heartbeat(computation_id, timeout_seconds) do
    import Ecto.Query
    now = System.system_time(:second)

    from(c in Journey.Persistence.Schema.Execution.Computation,
      where: c.id == ^computation_id and c.state == :computing
    )
    |> Journey.Repo.update_all(set: [
      last_heartbeat_at: now,
      heartbeat_deadline: now + timeout_seconds
    ])
  end
end
```

Note: Simple loop instead of GenServer - no need for GenServer overhead for a fire-and-forget process.

**TODO (Future Work):** Wrap `Repo.update_all` in `try/rescue` with retry logic. Currently, a DB error will crash the Watchdog (and thus the linked worker). See spec "Future Work" section.

### 4.2 Spawn Watchdog Inside Worker Task
**File:** `lib/journey/scheduler.ex`

In `launch_computation/3`, the watchdog must be spawned INSIDE the Task.start block (lines 60-128), at the very beginning, linked to the Task process:

```elixir
Task.start(fn ->
  prefix = "[#{execution.id}.#{computation.node_name}.#{computation.id}] [#{execution.graph_name}]"
  Logger.debug("#{prefix}: starting async computation")

  graph = Journey.Graph.Catalog.fetch(execution.graph_name, execution.graph_version)
  graph_node = Journey.Graph.find_node_by_name(graph, computation.node_name)

  # NEW: Spawn linked watchdog - dies when this Task dies
  _watchdog_pid = spawn_link(fn ->
    Journey.Scheduler.Watchdog.run(
      computation.id,
      graph_node.heartbeat_interval_seconds,
      graph_node.heartbeat_timeout_seconds
    )
  end)

  # ... rest of existing code (input_versions_to_capture, f_compute call, etc.)
end)
```

The `spawn_link` ensures:
- Watchdog starts immediately when computation begins
- Watchdog dies when worker Task completes (success/failure/crash)
- Worker dies if Watchdog crashes (intentional—we want the worker to stop if heartbeats stop)

**Important:** Because of `spawn_link`, the Watchdog must be resilient to transient failures. If `update_heartbeat` crashes due to a temporary DB issue, it would kill the worker. See "Future Work" in the spec for planned improvements (retry logic, `try/rescue` error handling).

---

## Phase 5: Update Abandonment Sweeper

**File:** `lib/journey/scheduler/background/sweeps/abandoned.ex`

### 5.1 Update Query (lines 113-128)
Change from:
```elixir
where: c.state == :computing and not is_nil(c.deadline) and c.deadline < ^current_time
```

To:
```elixir
where: c.state == :computing and (
  # Hard timeout (existing logic)
  (not is_nil(c.deadline) and c.deadline < ^current_time) or
  # Heartbeat timeout (new logic) - heartbeat_deadline is set per-computation
  (not is_nil(c.heartbeat_deadline) and c.heartbeat_deadline < ^current_time)
)
```

**Note:** Since `heartbeat_deadline` is stored on the computation record (and extended on each heartbeat), the sweeper doesn't need to look up graph config or calculate timeouts. Simple comparison.

---

## Phase 6: Testing

### 6.1 Unit Tests
- Watchdog starts and sends pulses
- Watchdog dies when linked process dies
- Heartbeat updates `last_heartbeat_at` in DB

### 6.2 Integration Tests
- Computation with heartbeat completes normally
- Computation abandoned when heartbeat stops (simulate crash)
- Legacy computations (nil heartbeat_deadline) only use hard deadline
- Validation rejects invalid heartbeat config (interval > timeout/2)

### 6.3 Test Files
- `test/journey/scheduler/watchdog_test.exs` (new)
- `test/journey/scheduler/background/sweeps/abandoned_test.exs` (update)
- `test/journey/graph/validations_test.exs` (update)

---

## Files to Modify

| File | Change |
|------|--------|
| `priv/repo/migrations/*` | New migration (add `last_heartbeat_at`, `heartbeat_deadline`) |
| `lib/journey/persistence/schema/execution/computation.ex` | Add 2 fields |
| `lib/journey/graph/step.ex` | Add 2 struct fields |
| `lib/journey/node.ex` | Extract options (6 functions: compute, mutate, tick_once, tick_recurring, historian, archive) |
| `lib/journey/graph/validations.ex` | Add validation rule |
| `lib/journey/scheduler/available.ex` | Initialize heartbeat fields |
| `lib/journey/scheduler/watchdog.ex` | **New module** |
| `lib/journey/scheduler.ex` | Spawn watchdog in Task |
| `lib/journey/scheduler/background/sweeps/abandoned.ex` | Update query |

---

## Migration Strategy (Legacy Computations)

Legacy computations (created before this deployment) will have:
- `last_heartbeat_at: nil`
- `heartbeat_deadline: nil`

The sweeper query checks `not is_nil(c.heartbeat_deadline)` before comparing, so legacy computations are unaffected by heartbeat logic - they continue to use only the hard `deadline` timeout.

New computations get both `deadline` and `heartbeat_deadline` set on start, and `heartbeat_deadline` is extended on each pulse.

---

## Implementation Order

1. Migration + Schema (can be done independently)
2. Step struct + Node options + Validation (can be done together)
3. Initialize heartbeat in available.ex
4. Watchdog module
5. Integrate watchdog spawning in scheduler.ex
6. Update abandonment sweeper query
7. Tests throughout
