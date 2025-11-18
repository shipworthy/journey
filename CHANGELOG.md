# Changelog

## v0.10.39

- **API Ergonomics Improvements**: renaming `Journey.Tools.summarize_as_text/1` -> `Journey.Tools.introspect/1`, renaming `Journey.start_execution/1` -> `Journey.start/1`, adding simple `Journey.new_graph(nodes)` with auto-generated graph name and revision. (#224)

## v0.10.38

- **Documentation Updates**: 
  * `Journey.get_value/3` is now marked as deprecated in favor of `Journey.get/3`. The function remains fully functional for backward compatibility. (#219)
  * Reorganized and simplified documentation - created new `BASIC_CONCEPTS.md` with comprehensive examples, refactored README.md for improved clarity and focus on practical use cases. (#220, #221)
- **Internal Enhancement**: Streamlined license validation logic for improved code maintainability. Added support for configurable license key service URL via `JOURNEY_LICENSE_KEY_SERVICE_URL` environment variable. (#222)

## v0.10.37

- **API Naming**: Renamed `schedule_once/4` to `tick_once/4` and `schedule_recurring/4` to `tick_recurring/4` to better reflect that these nodes emit time-based "ticks" (revisions). The old function names remain available with deprecation warnings for backward compatibility. Both old and new internal type atoms (`:schedule_once`, `:tick_once`, `:schedule_recurring`, `:tick_recurring`) are supported for zero-downtime deployments.
- **New API**: `Journey.count_executions/1` - Returns the count of executions matching specified criteria using database-level counting (SQL COUNT) without loading records into memory. Supports the same filtering capabilities as `list_executions/1`. (#217)
- **Enhanced Filtering**: `Journey.list_executions/1` now supports `:is_set` and `:is_not_set` filter operators to check whether a node has been set, regardless of its value. (#214, #215)
- **Error Handling, logging**: `Journey.Tools` functions (`computation_state/2`, `computation_status_as_text/2`, `summarize_as_data/1`) now raise `ArgumentError` with clear messages when execution is not found. Extra logging in scheduler for better debuggability. (#216, #211)
- **Elixir 1.19 Support**: Updates for Elixir 1.19 compatibility: compiler warning fixes, CI updates. (#212, #216)

## v0.10.36

- `mutate()` nodes: now accept `update_revision_on_change: true` option, to have the mutated node trigger downstream computations (idempotent behavior matching `Journey.set/3`). (#207)
- `compute()` nodes do not trigger downstream computations if the newly computed value is unchanged (idempotent behavior, matching `Journey.set/3`). (#208)
- `schedule_recurring()` nodes: recompute when upstream dependencies change, adding new semantics for pausing recurring scheduling (node's `f_compute` returning `{:ok, 0}`). (#203, #204)
- relaxing `ex_doc` version constraint to include 0.39. (#205)

## v0.10.33 - v0.10.35

- Metadata support: `Journey.set/3` now accepts `metadata: %{...}` option for tracking additional context.
- Multinodal nodes: Added `f_compute/2` for multi-input compute nodes and enhanced `historian()` for tracking node changes (EXPERIMENTAL).
- `Journey.get/3`: New function returning both value and revision (soft-deprecates `get_value/3`).
- `Journey.new_graph/4`: Added optional `execution_id_prefix: "..."` parameter for easier troubleshooting.
- `Journey.values/2`: Added `include_unset_as_nil: true` option.
- `Journey.get_value/3`: Enhanced with more ergonomic `wait:` and `timeout:` options, added `wait_for_revision:`, superseded by `Journey.get/3`.
- Fully implemented reactive behavior for `:or` recomputations.
- `schedule_once/3` nodes now recompute on new upstream revisions.

## v0.10.32

- Graph validation: `Journey.new_graph/4` to explicitly check for circular dependencies.
- `Journey.list_executions/1`'s `filter_by:` param to handle `:contains`, `:icontains`, `:list_contains`.
- Renamed `Journey.set_value()` to `Journey.set/2` / `Journey.set/3` and `Journey.unset_value()` to `Journey.unset/2`, added support for atomically setting multiple values.
- Documentation updates (graph migration notes, modules and functions summary, tidier license text).

## v0.10.31
- Expanding possible versions of [`ecto`](https://hexdocs.pm/ecto) and [`ecto_sql`](https://hexdocs.pm/ecto_sql) to include `3.13`, in addition to `3.12`.
- Hardening handling of computations that didn't complete due to various conditions (infrastructure reboots, crashes, redeployments, failures).
