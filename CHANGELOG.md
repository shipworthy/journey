# Changelog

## v0.10.56

- **`mutate()` multi-target**: `mutates:` now accepts a list of nodes, so a single `mutate()` node can mutate multiple target nodes (#325)
- **`Journey.Tools.computation_state()`**: now handles archived executions (#324)
- **License**: updating license metadata to include the SPDX `LicenseRef-` prefix (#323)
- **Dependencies**: removing the dependency on `:number` (reimplemented inline); updating decimal, ecto, ecto_sql, postgrex, and ex_doc (#326)

## v0.10.55

- **`loop()` node type**: adding a `loop()` node type, with associated tightening of `f_on_save` behavior and docs. `f_on_save` is no longer fired on retries, only when the computation completes with a value or an error. This could be a breaking change if your code relied on being notified of retries. (#316, #322)
- **Stable ordering**: using a stable order when fetching values and computations in an execution (#318)
- **Mermaid diagrams**: rendering node types as atoms (#314)
- **Livebooks**: updating livebooks to reflect changes in `Journey.Tools.introspect()` (#313)
- **README**: mentioning modeling of stateful things (#315)
- **Test stability**: faster, more deterministic tests (#317); hardening the `mutate()` doctest (#319)
- **Dependencies**: picking up updated packages — credo, db_connection, decimal, ecto, ex_doc, jason, makeup_erlang, postgrex, telemetry (#320)

## v0.10.54

- **`Journey.Tools.introspect/1`**: improved rendering of failed computations, now including inputs and error details (#311)
- **Livebook examples**: adding a livebook illustrating Journey's retries (#310)
- **Documentation**: setting `@doc false` for internal modules and functions (#309)

## v0.10.53

- **Livebook examples**: adding livebooks for `compute()`, `mutate()`, `historian()`, `tick_once()`, `tick_recurring()`, and `archive()` nodes (#294, #295, #296, #297, #300, #301, #304, #306); retiring `basic.livemd` (#299)
- **`Journey.start/1`**: documenting `Journey.start/1` as the preferred API for starting executions; soft-deprecating `Journey.start_execution/1` (old code continues to work without warnings) (#307)
- **`archive()` node type**: `archive()` nodes now have their own underlying `:archive` node type; existing archive nodes continue to operate unchanged (#305)
- **Mermaid diagrams**: nodes rendered as inactive when their dependencies aren't met (#303)
- **Documentation**: defining groups for extras and function docs (#298), tidying docs for `mutate()` (#292)
- **Test stability**: more resilient sweeper and conditional-clearing tests (#293, #302)

## v0.10.52

- **Mermaid diagrams**: more robust rendering of compute function names (#289)
- **Build key check**: adding `:inets` and `:ssl` to `extra_applications` for proper OTP startup (#290)

## v0.10.51

- **Mermaid diagram improvements**: adding `Journey.Tools.generate_mermaid_execution/2` for visualizing executions (#285, #287), streamlined colors, shapes, and legend (#286)
- **README improvements**: streamlining content, better structured links to resources (blogs, examples, references) (#284)

## v0.10.50

- **`f_on_save` for `input()` nodes**: `input()` nodes now support `f_on_save` callbacks; graph-wide `f_on_save` is now also invoked when input node values change. This might be a breaking change for some graphs. (#279)
- **`f_on_save/3` API**: adding `f_on_save/3` and deprecating `f_on_save/2`, for a simpler API (#278)
- **`compute()` docs**: improving documentation for `f_compute` (#282)
- **Misc updates**: docs, deps, Elixir 1.19.5, license verbiage (#280)
- **README fix**: fixing the name of Sasa Juric's talk (#281)

## v0.10.49

- **Scheduler: fix max_retries edge condition**: fixing an edge condition where `max_retries` had no effect, test updates (#276)

## v0.10.48

- **Introspection: computation times**: `Journey.Tools.introspect/1` output now includes computations' times (#274)
- **Garbage collection for old computations**: implementing garbage collection for old computations (#273)
- **Documentation fix**: fixing docs for the `archive()` node (#272)
- **Consistent error handling**: various `Journey.*` functions now raise a consistent `ArgumentError` when the supplied execution id does not match an existing execution (#271)
- **More deterministic OR recompute test**: making an OR recompute step more deterministic (#270)
- **API ergonomics**: allow `Journey.get(execution_id, ...)`, `Journey.values()` and `values_all()` to also accept execution id (#268, #269)
- **Idempotent nil re-set**: making re-setting nil value idempotent, just like any other value (#267)

## v0.10.47

- **Assorted scheduler refinements**: scoping retry counter to current cycle (#264); quieted sweeper "no work" logging, log config on startup, test coverage (#265)

## v0.10.46

- **tick_recurring: improvements**: better reliability and precision (#260, #262)
- **Trim steady state logs from sweepers**: reducing log noise by suppressing sweeper logs when no work is being done (#259)
- **Test stability improvements**: improving scheduler and schedule invalidation test stability (#258, #261)

## v0.10.45

- **Scheduler: skip loading inactive computations**: important optimization for executions with a long history of computations (#256)
- **Journey.Tools: harden introspection for orphaned nodes**: introspection functions now handle computation nodes that no longer exist in the graph (#255)
- **Journey.set() deterministic return value**: `Journey.set/3` now returns a deterministic execution from before kicking off downstream computations (#254)
- **Scheduler: prevent duplicate computation scheduling**: closing a window that allowed occasional mis-scheduling of duplicate computations (#251)
- **Test stability and isolation improvements**: various tests updated to use deterministic waits and better isolation (#249, #250, #252)
- **Dependency updates**: picking up updated libraries, addressing Elixir warnings, updating ex_doc to 0.40.1 (#247, #248)

## v0.10.44

- **Test stability updates**: using deterministic `Journey.get(..., wait: {:newer_than, prev_revision})` in various tests to make them deterministic / stable (#244, #245)
- **Preserve Historian node value across invalidations**: :historian nodes are accumulators, and should keep their value even when their upstream dependencies are not satisfied (#242, #243)

## v0.10.43

- **A Minor License text update**: updating the year to 2026 (9b2001f).

## v0.10.42

- **better DevEx: validate node options**: this helps to avoid typos and misspellings in graph definitions (#238)
- **adding heartbeats for monitoring computations of self-computing nodes**: this allows for faster detection and more robust handling of abandoned computations (#237)
- **ops tooling: Journey.Tools.abandon_computation/1**: adding `Journey.Tools.abandon_computation/1` for abandoning a computation (#236)
- **refactor/simplification**: refactoring Journey.Executions into smaller more focused submodules, adding `GEMINI.md` (#235)
- **singleton executions**: implementing singleton executions, with `singleton: true` param to `Journey.new_graph/2` graph attribute and `Journey.find_or_start/1` (#234)

## v0.10.41

- **documentation updates**: updating links and verbiage to reference the new repo (https://github.com/shipworthy), updating package description. (#229, #230, #231)

## v0.10.40

- **refinements**: adding `jason` as an explicit dependency, renaming the `UselessMachine` example to `Journey.Examples.UselessMachine`. (#226, #227)

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
