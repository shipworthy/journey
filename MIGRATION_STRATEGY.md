# Updating Graphs
## I need to change my graph. How?

If the change **does not break** the flow, simply make the change, test, and deploy it.

If the change **breaks** the flow, create a new graph, with a new name and/or version. Switch your code to create executions of the new graph instead of the old graph, and new executions will follow the new flow.

As long as the old graph continues to be registered, old executions will continue to navigate the flow defined in the old graph.

Examples of changes that might **break** the flow:
* Changing upstream dependencies on pre-existing computation nodes.
* Removing or renaming a node.

Anything that would have the current executions go "Huh? Now what do I do??!?" is likely to be a breaking change, that would require creating a new graph.

However, if you don't care about the state of current or old executions, you can archive them all (`Journey.list_executions(graph_name: graph.name, graph_version: graph.version) |> Enum.each(fn e -> Journey.archive(e.id) end)`), and simply change your existing graph, even if changes are major.

## Examples

### A minor change (no new graph needed)

Updating this graph

```elixir
graph = Journey.new_graph(
  "zodiac",
  "v1.0.0",
  [
    input(:name),
    input(:birthday),
    compute(:zodiac_sign, [:name, :birthday], &compute_zodiac/1)
  ]
)
```

with a couple of extra independent or downstream nodes is **not a breaking change**, and it does not require a new graph definition:

```elixir
graph = Journey.new_graph(
  "zodiac",
  "v1.0.0",
  [
    input(:name),
    input(:birthday),
    # new input node:
    input(:pet_preference),
    compute(:zodiac_sign, [:name, :birthday], &compute_zodiac/1),
    # new computation:
    compute(:horoscope, [:zodiac_sign, :pet_preference], &compute_horoscope/1)
  ]
)
```

If a graph was updated to include new nodes, the executions of this graph will be upgraded to include those new nodes when they are loaded.


### A major change (need a new graph)

If you need to make `:zodiac_sign` dependent on having the user's credit card number (for some reason), you'll likely need a new graph (or have existing executions enter an ambiguous state).

```elixir
graph = Journey.new_graph(
  "zodiac",
  # new version!
  "v2.0.0",
  [
    input(:name),
    input(:birthday),
    # new input node:
    input(:cc),
    # new upstream prerequisite:
    compute(:zodiac_sign, [:name, :birthday, :cc], &compute_zodiac/1)
  ]
)
```

Then make sure your configuration registers both graphs:

```elixir
config :journey, :graphs, [
  &MyApp.Graphs.Zodiac.V1.graph/0,
  &MyApp.Graphs.Zodiac.V2.graph/0
]
```

The strategy described here applies to evolving a graph due to changing requirements or fixing of bugs in the code or in the flow.