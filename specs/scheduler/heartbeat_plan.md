# Implementation Summary: Heartbeat for Journey Computations

Based on `JOURNEY_HEARTBEAT_SPEC.md`. This document summarizes the implemented changes.

## Problem
Long-running jobs require large `abandon_after_seconds`. If the worker crashes early, the system won't detect it until the full timeout expires.

## Solution
Decouple crash detection (heartbeat) from max duration (deadline). Heartbeat process sends periodic pulses; sweeper detects missing heartbeats.

---

## Changes Made

### 1. Database Migration
**File:** `priv/repo/migrations/20251229013841_add_heartbeat_fields_to_computations.exs`

```elixir
alter table(:computations) do
  add :last_heartbeat_at, :bigint, null: true
  add :heartbeat_deadline, :bigint, null: true
end
```

### 2. Schema Update
**File:** `lib/journey/persistence/schema/execution/computation.ex`

Added fields:
```elixir
field(:last_heartbeat_at, :integer, default: nil)
field(:heartbeat_deadline, :integer, default: nil)
```

### 3. Step Struct
**File:** `lib/journey/graph/step.ex`

Added to defstruct and @type:
```elixir
:heartbeat_interval_seconds,
:heartbeat_timeout_seconds
```

### 4. Node Functions
**File:** `lib/journey/node.ex`

Updated `compute/4`, `mutate/4`, `tick_once/4`, `tick_recurring/4`, `historian/3`, `archive/3` to accept:
```elixir
heartbeat_interval_seconds: Keyword.get(opts, :heartbeat_interval_seconds, 70),
heartbeat_timeout_seconds: Keyword.get(opts, :heartbeat_timeout_seconds, 240)
```

### 5. Graph Validation
**File:** `lib/journey/graph/validations.ex`

Added `validate_heartbeat_options/1`:
- `heartbeat_interval_seconds` must be >= 30 seconds
- `heartbeat_interval_seconds` must be <= `heartbeat_timeout_seconds / 2`

### 6. Initialize Heartbeat on Computation Start
**File:** `lib/journey/scheduler/available.ex`

In `grab_this_computation/4`, added to changeset:
```elixir
heartbeat_deadline: now + graph_node.heartbeat_timeout_seconds
```

Note: `last_heartbeat_at` is left as `nil` initially; updated on first heartbeat pulse.

### 7. Heartbeat Module
**File:** `lib/journey/scheduler/heartbeat.ex` (new)

Key design decisions:
- Uses `spawn_link` to link heartbeat to worker (mutual termination)
- Uses `Process.flag(:trap_exit, true)` to receive EXIT messages from worker
- Uses `receive` with `after` for interruptible sleep
- Includes ±20% jitter on intervals to prevent thundering herd
- Includes 10-second deadline buffer to let sweep act first
- Can mark computation as abandoned and kill worker when hard deadline exceeded
- Wraps DB operations in `try/rescue` for resilience

### 8. Scheduler Integration
**File:** `lib/journey/scheduler.ex`

Refactored `launch_computation/3`:
- Extracted `worker_with_heartbeat/4` function
- Spawns heartbeat as linked sibling inside the Task
- Heartbeat receives execution_id, computation_id, node_name, interval, and timeout

### 9. Abandonment Sweeper
**File:** `lib/journey/scheduler/background/sweeps/abandoned.ex`

Updated query to check both deadlines:
```elixir
where:
  c.state == :computing and
    ((not is_nil(c.deadline) and c.deadline < ^current_time) or
       (not is_nil(c.heartbeat_deadline) and c.heartbeat_deadline < ^current_time))
```

---

## Tests
**File:** `test/journey/scheduler/heartbeat_test.exs`

Coverage:
- Graph validation (rejects bad config, accepts valid config)
- Heartbeat initialization (fields set on computation start)
- Heartbeat during execution (fields updated during long-running computation)
- Deadline enforcement (worker killed when deadline exceeded)

---

## Files Modified

| File | Change |
|------|--------|
| `priv/repo/migrations/20251229013841_*` | New migration |
| `lib/journey/persistence/schema/execution/computation.ex` | Add 2 fields |
| `lib/journey/graph/step.ex` | Add 2 struct fields + types |
| `lib/journey/node.ex` | Extract options in 6 functions |
| `lib/journey/graph/validations.ex` | Add validation rule |
| `lib/journey/scheduler/available.ex` | Initialize heartbeat_deadline |
| `lib/journey/scheduler/heartbeat.ex` | **New module** |
| `lib/journey/scheduler.ex` | Spawn heartbeat, refactor to worker_with_heartbeat |
| `lib/journey/scheduler/background/sweeps/abandoned.ex` | Update query |
| `test/journey/scheduler/heartbeat_test.exs` | **New test file** |
| `mix.exs` | Coverage threshold 81 → 82 |
