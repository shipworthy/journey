# UnblockedBySchedule sweep has a near-zero detection window for tick_recurring nodes with period >= 5 * sweeper_period

## Summary

The `UnblockedBySchedule` sweep uses a `set_time >= cutoff_time` filter that creates a detection window exactly equal to `5 * sweeper_period_seconds`. For any `tick_recurring` node whose period is >= that window (e.g., a 5-minute tick with a 60-second sweeper), the detection window shrinks to ~1 second, causing the sweep to miss the due pulse ~98% of the time. The system falls back to the `StalledExecutions` sweep (30-minute cycle), resulting in tick delays of 8-43 minutes instead of the expected <60 seconds.

## Affected code

- `lib/journey/scheduler/background/sweeps/unblocked_by_schedule.ex` lines 11-37

## Related ticket

- `schedule_nodes_misses_computations_created_by_regenerate_recurring.md` — a second bug that compounds this one. Even when `RegenerateScheduleRecurring` correctly creates a `:not_set` computation after the pulse is due, `ScheduleNodes` misses it because the execution's `updated_at` wasn't touched.

## Observed behavior

On Ooshki staging (Cloud Run, revision `ooshki26-staging-00255-8d7`), the `in_5_minutes` tick_recurring node (System graph) exhibited a 777-second (13-minute) gap:

- **00:51:31 UTC** — `in_5_minutes` computation succeeds, sets `node_value = ~00:56:31` (epoch), `set_time = 00:51:31` (epoch)
- **00:56:31 UTC** — pulse becomes due
- **00:52:29 through 01:03:29 UTC** — `UnblockedBySchedule` runs every 60 seconds on this revision, each time reporting `"no recently due pulse value(s) found"`
- **01:04:29 UTC** — `StalledExecutions` sweep catches the stalled execution, advances 3 executions, and the `process_incoming_emails` computation finally runs

The expected behavior is that `UnblockedBySchedule` detects the due pulse within one sweep cycle (~60 seconds) after the pulse time passes.

## Root cause analysis

### The query

`UnblockedBySchedule.q_execution_ids_to_advance/2` (lines 11-37):

```elixir
now = System.system_time(:second)
time_window_seconds = 5 * sweeper_period_seconds    # 5 * 60 = 300
cutoff_time = now - time_window_seconds              # now - 300

from(e in executions_for_graphs(execution_id, all_graphs),
  join: c in assoc(e, :computations),
  join: v in Value,
  on: ...,
  where:
    c.state == :success and
    not is_nil(v.set_time) and
    fragment("?::bigint", v.node_value) > 0 and
    fragment("?::bigint", v.node_value) <= ^now and      # pulse is due
    v.set_time >= ^cutoff_time,                          # set_time within window
  ...
)
```

### The timing collision

For `in_5_minutes` (`system.ex:54-57`):

```elixir
defp in_5_minutes(_values) do
  {:ok, System.system_time(:second) + 60 * 5}    # now + 300
end
```

When the computation succeeds at time **T**:
- `v.set_time` = **T** (when the value was persisted)
- `v.node_value` = **T + 300** (the pulse timestamp, 5 minutes in the future)

The pulse becomes due at **T + 300**. At that moment, the `UnblockedBySchedule` query evaluates:

| Sweep time | `node_value <= now`? | `set_time >= now - 300`? | Detected? |
|---|---|---|---|
| T + 295 | T+300 <= T+295 = **No** | T >= T-5 = Yes | No (not due) |
| T + 300 | T+300 <= T+300 = **Yes** | T >= T+0 = **Yes** (exactly equal) | **Yes** |
| T + 301 | T+300 <= T+301 = Yes | T >= T+1 = **No** | No |
| T + 360 | T+300 <= T+360 = Yes | T >= T+60 = **No** | No |

**The detection window is 1 second wide.** With a 60-second sweep period, the probability of landing in that window is ~1/60 (1.7%).

### Why it's exactly at the boundary

The `set_time` filter uses a window of `5 * sweeper_period` = `5 * 60` = **300 seconds**. The tick period is `60 * 5` = **300 seconds**. These are identical. So by the time the pulse becomes due (300 seconds after `set_time`), the `set_time` has aged out of the detection window.

This affects any `tick_recurring` whose period >= `5 * sweeper_period_seconds`. With the default 60-second sweeper:
- 5-minute tick: **1-second detection window** (the exact boundary case observed here)
- 10-minute tick: **0-second detection window** (always missed)
- Weekly tick: **0-second detection window** (always missed)
- 2-minute tick: 120-second detection window (usually caught)
- 1-minute tick: 240-second detection window (reliably caught)

### What other sweeps do during the gap

The full sweep order in `periodic.ex:67-76`:

1. **`Abandoned`** — looks for `state: :computing` with expired heartbeats. Not relevant (the computation is `:success`, not `:computing`).

2. **`ScheduleNodes`** — looks for `c.state == :not_set`. Not relevant because the tick computation is already `:success`. It already computed its pulse value; the issue is that nobody is detecting the pulse is now due.

3. **`UnblockedBySchedule`** — **fails** due to the `set_time` window issue described above.

4. **`RegenerateScheduleRecurring`** — creates new `:not_set` computations for the _next_ cycle. It requires `v.node_value <= now` (pulse time has passed) AND `not exists(... c2.state == :not_set)` (no pending computation already exists for this node). This sweep **did work correctly**: at 00:57:02, revision 00256's `RegenerateScheduleRecurring` detected the due pulse and created a new `:not_set` computation. However, `ScheduleNodes` then failed to find that `:not_set` due to a separate bug (see `schedule_nodes_misses_computations_created_by_regenerate_recurring.md`).

5. **`MissedSchedulesCatchall`** — runs once per 23 hours at a preferred hour (default: 2 AM UTC). Not available for routine gap recovery. Also has a `@recent_boundary_minutes 25` filter that excludes pulses due less than 25 minutes ago.

6. **`StalledExecutions`** — **this is what catches it**. Throttled at 30-minute intervals. Checks `e.updated_at` within a time window, excluding executions updated in the last 10 minutes (`@too_new_threshold_seconds = 10 * 60`). The execution's `updated_at` was set at 00:51:31 (when the tick computation completed), so it becomes eligible after 01:01:31 (10 min later). It was caught at 01:04:29 when the next 30-minute throttle window opened.

### Why StalledExecutions delay varies

The actual gap depends on alignment with the `StalledExecutions` 30-minute throttle cycle:
- The execution becomes eligible 10 minutes after `set_time` (the `@too_new_threshold_seconds` filter)
- Then it must wait for `StalledExecutions` to win its next 30-minute throttle window
- Worst case: 10 min (too-new filter) + 30 min (throttle cycle) = **40 minutes**
- Best case: 10 min + 0 min = **10 minutes**
- Observed: 13 minutes (01:04:29 - 00:51:31)

## Evidence from logs

### Log source

Analysis was performed on GCP Cloud Run staging logs downloaded by the monitoring tool at `ooshki26/monitoring/`. The gap window (00:50 to 01:06 UTC, 2026-02-20) was found in these log files:

- `monitoring/logs/log_20260219_165114.json`
- `monitoring/logs/log_20260219_170138.json`
- `monitoring/logs/log_20260219_171202.json`
- `monitoring/logs/log_20260219_172219.json`

Analysis scripts used:
- `scripts/investigate_tick_gap2.exs` — primary investigation (targeted the correct log files)
- `scripts/check_log_coverage.exs` — identified which log files contained the gap window
- `scripts/analyze_journey_hour.exs` — initial broad analysis that surfaced the 777s anomaly

### Key log entries from the gap window

**`in_5_minutes` fires, then goes silent for 13 minutes:**
```
2026-02-20T00:51:31.076Z  [ooshki26-staging-00255-8d7]  in_5_minutes
2026-02-20T01:04:29.857Z  [ooshki26-staging-00255-8d7]  in_5_minutes   (next occurrence, after StalledExecutions rescue)
```

**`UnblockedBySchedule` runs every 60s and finds nothing:**
```
2026-02-20T00:52:29.451Z  UnblockedBySchedule  no recently due pulse value(s) found
2026-02-20T00:53:29.688Z  UnblockedBySchedule  no recently due pulse value(s) found
2026-02-20T00:54:30.101Z  UnblockedBySchedule  no recently due pulse value(s) found
... (continues every ~60s through 01:03:29) ...
2026-02-20T01:03:29.217Z  UnblockedBySchedule  no recently due pulse value(s) found
```

**`ScheduleNodes` throttled on revision 00255 because revision 00256 holds the lock:**
```
2026-02-20T00:52:29.319Z  [00255-8d7]  ScheduleNodes  skipping - time_check: last run too recent
2026-02-20T00:53:29.537Z  [00255-8d7]  ScheduleNodes  skipping - time_check: last run too recent
```

**Revision 00256 runs `ScheduleNodes` but also finds nothing:**
```
2026-02-20T00:53:19.164Z  [00256-szc]  ScheduleNodes  no recently due pulse value(s) found
2026-02-20T00:55:20.008Z  [00256-szc]  ScheduleNodes  no recently due pulse value(s) found
```

**`StalledExecutions` rescues at 01:04:29:**
```
2026-02-20T01:04:29.667Z  StalledExecutions  completed. attempted to advance 3 execution(s)
```

**`RegenerateScheduleRecurring` has nothing to do throughout the gap:**
```
2026-02-20T00:52:29.483Z  RegenerateScheduleRecurring  no schedule_recurring nodes need regeneration
... (consistent through the gap) ...
```

### Multi-revision context

Two revisions were active during the gap window:
- `ooshki26-staging-00255-8d7` — 314 log entries in window
- `ooshki26-staging-00256-szc` — 189 log entries in window

Both share the same PostgreSQL database and `sweep_runs` table. The `ScheduleNodes` sweep on revision 00255 was consistently throttled because revision 00256 was running it within the 120-second minimum interval. However, this is not the root cause — even revision 00256's `ScheduleNodes` runs reported "no recently due pulse." The root cause is that neither `ScheduleNodes` (which looks for `:not_set` computations) nor `UnblockedBySchedule` (which has the `set_time` window issue) could detect the due pulse.

## Proposed fixes

### Option A: Use `node_value` instead of `set_time` for the recency filter (recommended)

The `set_time >= cutoff_time` filter's intent is to avoid reprocessing ancient historical schedule values. The same goal is achieved by checking when the pulse _became due_ rather than when it was _set_:

```elixir
# Current (broken for long-period ticks):
v.set_time >= ^cutoff_time

# Proposed:
fragment("?::bigint", v.node_value) >= ^cutoff_time
```

This means "pulse became due within the last 300 seconds" rather than "value was set within the last 300 seconds." A 5-minute tick's pulse that became due 30 seconds ago would reliably pass the filter. The detection window becomes `time_window_seconds` (300s) regardless of the tick period.

**Trade-off**: Ancient schedule values whose pulse time falls within the last 300 seconds would also be picked up. This seems acceptable since the query already filters for `c.state == :success` (the computation ran) and `node_value <= now` (it's due).

### Option B: Increase the time window multiplier

```elixir
# Current:
time_window_seconds = 5 * sweeper_period_seconds    # 300s

# Proposed:
time_window_seconds = 60 * sweeper_period_seconds   # 3600s (1 hour)
```

This would give a 5-minute tick a detection window of 3600 - 300 = 3300 seconds (~55 minutes). It would cover most practical tick periods but still fail for very long-period ticks (e.g., weekly).

**Trade-off**: A larger window means more historical values are scanned each cycle. Performance impact depends on how many completed schedule computations exist with `set_time` in the wider window.

### Option C: Remove the `set_time` filter entirely

```elixir
# Remove this line from the query:
v.set_time >= ^cutoff_time
```

Rely solely on `c.state == :success` and `fragment("?::bigint", v.node_value) <= ^now` to identify due pulses. The `node_value <= now` condition already prevents future pulses from being processed.

**Trade-off**: All historical schedule values with `state: :success` and `node_value` in the past would be candidates. This could cause unnecessary `advance()` calls on executions whose schedules were already processed. The downstream `advance()` should be idempotent (it reloads the execution and checks what actually needs computing), so this may be safe but wasteful.

### Option D: Add a separate, simpler query path for tick_recurring

Add a dedicated query for tick_recurring that doesn't use the `set_time` filter, keeping the existing `set_time` filter for `schedule_once` and `tick_once` (where the timing relationship is different):

```elixir
# For tick_recurring / schedule_recurring:
where:
  c.state == :success and
  v.node_type in [:schedule_recurring, :tick_recurring] and
  fragment("?::bigint", v.node_value) > 0 and
  fragment("?::bigint", v.node_value) <= ^now

# For tick_once / schedule_once (keep existing behavior):
where:
  c.state == :success and
  v.node_type in [:schedule_once, :tick_once] and
  v.set_time >= ^cutoff_time and
  fragment("?::bigint", v.node_value) > 0 and
  fragment("?::bigint", v.node_value) <= ^now
```

**Trade-off**: More complex query logic, but precisely targets the issue without changing behavior for schedule_once/tick_once nodes.

## Recommendation

**Option A** is the simplest and most correct fix. The `set_time` recency filter exists to avoid reprocessing old schedule values, but `node_value` (the pulse time) is a better proxy for "recency" since it represents when the action should happen, not when it was scheduled. A pulse that became due 30 seconds ago is always worth processing, regardless of when the computation that produced it ran.

## Reproduction

This issue is deterministic for any `tick_recurring` with period >= `5 * sweeper_period_seconds` (default: 300 seconds). To reproduce:

1. Register a graph with a `tick_recurring` node that returns `{:ok, System.system_time(:second) + 300}`
2. Start the background sweeper with default settings (60-second period)
3. Observe the tick_recurring's execution over multiple cycles
4. The `in_5_minutes` tick will fire, then `UnblockedBySchedule` will fail to detect the due pulse. `StalledExecutions` will rescue it 10-40 minutes later.

For faster reproduction, use a tick period of 6 minutes (360 seconds) which is clearly beyond the 300-second window, or observe the 5-minute tick over 10+ cycles — it will miss ~98% of them.

## Note on JSONB comparison

Some sweeps use `fragment("?::bigint", v.node_value)` for `node_value` comparisons (e.g., `UnblockedBySchedule`) while others use bare `v.node_value` (e.g., `RegenerateScheduleRecurring`, `MissedSchedulesCatchall`). This was initially suspected as a bug, but verified to be correct: Ecto encodes integer parameters as JSONB values (via the `JsonbScalar` type), so PostgreSQL performs JSONB-to-JSONB numeric comparison, which is well-defined. Confirmed with `SELECT '1740012991'::jsonb <= '1740013000'::jsonb` → `t` on the staging database. Adding `::bigint` casts for consistency is fine but not required for correctness.
