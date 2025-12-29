# Design Spec: Heartbeat & Watchdog for Journey Computations

## 1. Problem Statement
Currently, Journey uses a single `abandon_after_seconds` configuration to handle both **execution duration limits** and **crash detection**.

*   **The Conflict:** Long-running jobs (e.g., 5-hour scrapes) require a large `abandon_after_seconds`.
*   **The Consequence:** If the worker node crashes (OOM, restart, power loss) 1 minute into the job, Journey continues to respect the 5-hour lease. The job becomes a "Zombie"—dead but locked—stalling the entire workflow for hours.

## 2. Proposed Solution
Decouple "Proof of Life" from "Maximum Duration" by introducing an **Implicit Watchdog (Heartbeat)** mechanism.

### Key Concepts
1.  **Heartbeat Interval**: How often a running computation updates its "last seen" timestamp in the database (e.g., every 30s).
2.  **Heartbeat Timeout**: How long the system tolerates a missing heartbeat before declaring the computation dead (e.g., 90s).
3.  **Hard Deadline**: The existing `abandon_after_seconds` logic, retained as an absolute upper bound for total execution time.

## 3. Architecture

### 3.1 Data Model Changes
**Table:** `journey_computations` (Schema: `Journey.Persistence.Schema.Execution.Computation`)

Add two new columns:
*   `last_heartbeat_at`: `integer` (Unix timestamp, nullable).
    *   Initialize to `start_time` when state transitions to `:computing`.
    *   Updated periodically while running.
*   `heartbeat_deadline`: `integer` (Unix timestamp, nullable).
    *   Initialize to `start_time + heartbeat_timeout_seconds` when state transitions to `:computing`.
    *   Extended on each heartbeat pulse: `now + heartbeat_timeout_seconds`.
    *   **Design rationale**: Storing the absolute deadline (like existing `deadline` field) keeps the sweeper query simple - no graph lookups or timeout calculations needed at sweep time.

### 3.2 Component: The Watchdog Process
When `Journey.Scheduler.Worker` spawns a `Task` (or GenServer) to run the user's `f_compute` function, it must also spawn a **Watchdog** process linked to it.

**Watchdog Responsibilities:**
1.  **Monitor**: Link to the worker process.
2.  **Pulse**: Every `X` seconds (default: 30s), update the `last_heartbeat_at` field in the database for the current computation ID.
3.  **Terminate**:
    *   If the worker process finishes (success/failure), the Watchdog stops.
    *   If the worker process crashes, the Watchdog stops (and standard error handling takes over).

### 3.3 Component: The Sweeper (Scheduler)
The existing "Abandonment Sweep" logic must be updated to check two conditions for identifying stuck jobs.

**Current Logic:**
```elixir
if state == :computing AND (start_time + abandon_after_seconds < now) -> ABANDON
```

**New Logic:**
```elixir
is_hard_timeout      = (deadline < now)
is_heartbeat_expired = (heartbeat_deadline < now)

if state == :computing AND (is_hard_timeout OR is_heartbeat_expired) -> ABANDON
```

Since `heartbeat_deadline` is stored on the computation record (and extended on each pulse), the sweeper doesn't need to calculate timeouts or look up graph config.

## 4. Implementation Plan

### Phase 1: Database Migration
1.  Generate an Ecto migration to add `last_heartbeat_at` and `heartbeat_deadline` to the `journey_computations` table.
2.  Update `Journey.Persistence.Schema.Execution.Computation` schema with both fields.

### Phase 2: Watchdog Implementation
1.  Create `Journey.Scheduler.Worker.Watchdog`.
    *   It should be a simple `GenServer`.
    *   **Args**: `computation_id`, `pulse_interval`.
    *   **Logic**: Use `Process.send_after` to trigger a self-message `:pulse`.
    *   **DB Action**: `Repo.update_all` to set `last_heartbeat_at = now()` for the ID.
2.  Integrate into `Journey.Scheduler.Worker`.
    *   Start the Watchdog immediately after the transaction that sets state to `:computing`.
    *   Ensure the Watchdog is terminated when the computation completes.

### Phase 3: Scheduler Updates
1.  Modify `Journey.Scheduler.Background.Sweeps.Abandoned`.
2.  Update the Ecto query that looks for abandoned jobs.
3.  Example Query Logic:
    ```elixir
    from c in Computation,
      where: c.state == :computing,
      where: (not is_nil(c.deadline) and c.deadline < ^current_time) or
             (not is_nil(c.heartbeat_deadline) and c.heartbeat_deadline < ^current_time)
    ```

### Phase 4: Configuration & API
1.  Update `Journey.Node.compute/4` (and others) to accept new options:
    *   `:heartbeat_interval_seconds` (default: `70`).
    *   `:heartbeat_timeout_seconds` (default: `240`).
    *   **Note**: Heartbeats are enabled by default. For short-lived jobs (< 60s), the watchdog will simply terminate before the first pulse, incurring no database overhead.
2.  **Graph Validation**:
    *   **Rule**: `heartbeat_interval_seconds` MUST be less than or equal to `heartbeat_timeout_seconds / 2`.
    *   **Error Message**: "Node :my_node has unsafe heartbeat config. Interval (60s) must be <= half of timeout (100s) to prevent false positives."

## 5. Usage Example

```elixir
compute(
  :long_running_scrape,
  deps,
  &scrape/1,
  # Stop if the script hangs/loops for > 5 hours
  abandon_after_seconds: 60 * 60 * 5,
  
  # Check for pulses every 70 seconds (offset from default 60s abandon timeout)
  heartbeat_interval_seconds: 70,
  
  # Allow 3 missed heartbeats (plus buffer) before declaring death
  heartbeat_timeout_seconds: 240
)
```

## 6. Migration Strategy
*   Existing records will have `last_heartbeat_at: nil` and `heartbeat_deadline: nil`.
*   **Strategy**: The sweeper query checks `not is_nil(c.heartbeat_deadline)` before comparing, so legacy computations are unaffected by heartbeat logic.
*   **Effect**: Legacy computations continue to use only the hard `deadline` timeout. New computations get both `deadline` and `heartbeat_deadline` set on start, with `heartbeat_deadline` extended on each pulse.

## 7. Future Work (Out of Scope)
*   **Explicit Heartbeat**: Passing a callback to `f_compute` so user code can prove "logic liveness" (not just process liveness). The current plan only covers "Process/Node Liveness".
*   **Watchdog-Enforced Hard Deadline**: The Watchdog could monitor the computation's absolute `deadline` and forcibly kill the worker process when exceeded. This would catch runaway computations (infinite loops, hung I/O) that continue heartbeating but never complete. Currently, only the background sweeper detects hard deadline violations.
*   **Resilient DB Access in Watchdog**: The Watchdog's `update_heartbeat` call should include retry logic and error handling (e.g., `try/rescue` with logging). Transient DB failures (connection drops, timeouts) should not crash the Watchdog. Since the Watchdog is `spawn_link`ed to the worker (intentionally—we want the worker to die if heartbeats stop), making the Watchdog resilient to temporary DB issues prevents healthy workers from being killed by unrelated infrastructure hiccups.
