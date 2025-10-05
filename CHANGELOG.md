# Changelog

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
