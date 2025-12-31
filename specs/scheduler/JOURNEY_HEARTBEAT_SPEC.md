# Design Spec: Heartbeat & Watchdog for Journey Computations

## 1. Problem Statement
Currently, Journey uses a single `abandon_after_seconds` configuration to handle both **execution duration limits** and **crash detection**.

*   **The Conflict:** Long-running jobs (e.g., 5-hour scrapes) require a large `abandon_after_seconds`.
*   **The Consequence:** If the worker node crashes (OOM, restart, power loss) 1 minute into the job, Journey continues to respect the 5-hour lease. The job becomes a "Zombie"—dead but locked—stalling the entire workflow for hours.

## 2. Solution
Decouple "Proof of Life" from "Maximum Duration" by introducing an **Implicit Heartbeat** mechanism.

### Key Concepts
1.  **Heartbeat Interval**: How often a running computation updates its "last seen" timestamp in the database (default: 70s).
2.  **Heartbeat Timeout**: How long the system tolerates a missing heartbeat before declaring the computation dead (default: 240s).
3.  **Hard Deadline**: The existing `abandon_after_seconds` logic, retained as an absolute upper bound for total execution time.

## 3. Architecture

### 3.1 Data Model
**Table:** `journey_computations` (Schema: `Journey.Persistence.Schema.Execution.Computation`)

Two columns:
*   `last_heartbeat_at`: `integer` (Unix timestamp, nullable).
    *   Initialized to `nil` when state transitions to `:computing`.
    *   Updated on each heartbeat pulse.
*   `heartbeat_deadline`: `integer` (Unix timestamp, nullable).
    *   Initialized to `start_time + heartbeat_timeout_seconds` when state transitions to `:computing`.
    *   Extended on each heartbeat pulse: `now + heartbeat_timeout_seconds`.
    *   **Design rationale**: Storing the absolute deadline (like existing `deadline` field) keeps the sweeper query simple - no graph lookups or timeout calculations needed at sweep time.

### 3.2 Component: The Heartbeat Process
**Module:** `Journey.Scheduler.Heartbeat`

When `Journey.Scheduler` spawns a Task to run the user's `f_compute` function, it also spawns a **Heartbeat** process linked to it via `spawn_link`.

**Heartbeat Behavior:**
1.  **Link**: Spawned as a linked sibling to the worker process.
2.  **Trap Exit**: Uses `Process.flag(:trap_exit, true)` to receive EXIT messages when the worker exits.
3.  **Pulse**: Every `interval_seconds` (with ±20% jitter), updates `last_heartbeat_at` and extends `heartbeat_deadline` in the database.
4.  **Hard Deadline Enforcement**: If the heartbeat update fails because the hard `deadline` has passed, the heartbeat process marks the computation as abandoned and kills the worker via `exit(:computation_timeout)`.

**Exit Conditions:**
- Worker exits (normal or crash) → heartbeat receives `{:EXIT, pid, reason}` → exits immediately
- Computation state changes (completed/error/abandoned) → `update_heartbeat` returns 0 rows → exits
- Hard deadline exceeded → marks as abandoned, exits with `:computation_timeout` to kill worker
- Heartbeat process crashes → worker receives exit signal and dies (not trapping)

### 3.3 Component: The Sweeper
**Module:** `Journey.Scheduler.Background.Sweeps.Abandoned`

The abandonment sweep checks two conditions:

```elixir
is_hard_timeout      = (deadline < now)
is_heartbeat_expired = (heartbeat_deadline < now)

if state == :computing AND (is_hard_timeout OR is_heartbeat_expired) -> ABANDON
```

Since `heartbeat_deadline` is stored on the computation record (and extended on each pulse), the sweeper doesn't need to calculate timeouts or look up graph config.

### 3.4 Design Details

**Jitter:** Heartbeat intervals include ±20% randomization to prevent thundering herd when many computations pulse simultaneously.

**Deadline Buffer:** The heartbeat's `update_heartbeat` query includes a 10-second buffer (`deadline > now - 10`). This allows the background sweep to mark computations as abandoned first, avoiding races where both the heartbeat and sweep try to mark the same computation.

## 4. Configuration

### 4.1 Node Options
All step-type nodes (`compute`, `mutate`, `tick_once`, `tick_recurring`, `historian`, `archive`) accept:
*   `:heartbeat_interval_seconds` (default: `70`)
*   `:heartbeat_timeout_seconds` (default: `240`)

**Note:** Default interval is 70s (not 60s) to avoid coinciding with the default 60s `abandon_after_seconds`.

### 4.2 Validation Rules
*   `heartbeat_interval_seconds` MUST be >= 30 seconds.
*   `heartbeat_interval_seconds` MUST be <= `heartbeat_timeout_seconds / 2`.

## 5. Usage Example

```elixir
compute(
  :long_running_scrape,
  deps,
  &scrape/1,
  # Stop if the script hangs/loops for > 5 hours
  abandon_after_seconds: 60 * 60 * 5,

  # Check for pulses every 70 seconds
  heartbeat_interval_seconds: 70,

  # Allow ~3 missed heartbeats before declaring death
  heartbeat_timeout_seconds: 240
)
```

## 6. Migration Strategy
*   Existing records will have `last_heartbeat_at: nil` and `heartbeat_deadline: nil`.
*   **Strategy**: The sweeper query checks `not is_nil(c.heartbeat_deadline)` before comparing, so legacy computations are unaffected by heartbeat logic.
*   **Effect**: Legacy computations continue to use only the hard `deadline` timeout. New computations get both `deadline` and `heartbeat_deadline` set on start, with `heartbeat_deadline` extended on each pulse.

## 7. Future Work (Out of Scope)
*   **Explicit Heartbeat**: Passing a callback to `f_compute` so user code can prove "logic liveness" (not just process liveness). The current implementation only covers "Process/Node Liveness".
