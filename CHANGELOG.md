# Changelog

## v0.10.32

- Graph validation: `Journey.new_graph/4` to explicitly check for circular dependencies.
- `Journey.list_executions/1`'s `filter_by:` param to handle `:contains`, `:icontains`, `:list_contains`.
- Renamed `Journey.set_value()` to `Journey.set/2` / `Journey.set/3` and `Journey.unset_value()` to `Journey.unset/2`, added support for atomically setting multiple values.
- Documentation updates (graph migration notes, modules and functions summary, tidier license text).

## v0.10.31
- Expanding possible versions of [`ecto`](https://hexdocs.pm/ecto) and [`ecto_sql`](https://hexdocs.pm/ecto_sql) to include `3.13`, in addition to `3.12`.
- Hardening handling of computations that didn't complete due to various conditions (infrastructure reboots, crashes, redeployments, failures).
