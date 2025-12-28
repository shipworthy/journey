# Modules and Functions

Here is a brief summary of the modules and functions exposed by Journey. 

## `Journey` (Main Module)
The entry point for the Journey library. Provides functions for creating and managing computation graphs, starting and managing executions, and retrieving values from executions.

### Graph Management
- `Journey.new_graph/4` - Creates a new computation graph with the given name, version, and node definitions. Accepts options including `singleton: true` for singleton graphs.

### Execution Lifecycle
- `Journey.start_execution/1` - Starts a new execution instance of a computation graph (raises for singleton graphs)
- `Journey.find_or_start/1` - Returns existing execution or creates new for singleton graphs
- `Journey.load/2` - Loads the current version of an execution from the database
- `Journey.list_executions/1` - Queries and retrieves executions with filtering, sorting, and pagination
- `Journey.count_executions/1` - Returns the count of executions matching the specified criteria without fetching the execution records
- `Journey.archive/1` - Archives an execution, making it invisible and stopping all background processing
- `Journey.unarchive/1` - Un-archives the supplied execution if it is archived
- `Journey.history/1` - Returns the chronological history of all successful computations and set values

### Value Operations
- `Journey.set/2`, `Journey.set/3` - Sets values for input node(s) in an execution (potentially unblocking downstream computations)
- `Journey.unset/2` - Removes values from input nodes and invalidates all dependent computed nodes
- `Journey.get/3` - Returns the value of a node in an execution and its revision, optionally waits for the value to be set or updated

### Data Retrieval
- `Journey.values/2` - Returns a map of all set node values in an execution, excluding unset nodes
- `Journey.values_all/2` - Returns a map of all nodes in an execution with their current status, including unset nodes

## `Journey.Node`
Functions for creating various types of nodes in a graph.

Functions:
- `Journey.Node.input/1` - Creates a graph input node whose value is set with `Journey.set/3` or `Journey.set/2`
- `Journey.Node.compute/4` - Creates a self-computing node that calculates its value based on upstream dependencies, when unblocked
- `Journey.Node.mutate/4` - Creates a graph node that mutates the value of another node, when unblocked (optionally triggers downstream recomputation with `update_revision_on_change: true`)
- `Journey.Node.historian/3` - EXPERIMENTAL: Creates a history-tracking node that maintains a chronological log of changes to another node (default limit: 1000 entries)
- `Journey.Node.archive/3` - Creates a graph node that archives data when unblocked
- `Journey.Node.tick_once/4` - Creates a graph node that declares its readiness at a specific time, once
- `Journey.Node.tick_recurring/4` - Creates a graph node that declares its readiness at specific times, repeatedly

## `Journey.Node.Conditions`
Helper functions for use in graph definitions, when defining upstream dependencies for compute modules.

Functions:
- `Journey.Node.Conditions.provided?/1` - Checks if the supplied node has a value (for scheduled nodes, also checks timing)
- `Journey.Node.Conditions.true?/1` - Checks if the upstream node's value is true
- `Journey.Node.Conditions.false?/1` - Checks if the upstream node's value is false

## `Journey.Node.UpstreamDependencies`
Functions for defining complex conditions under which nodes are unblocked.

Functions:
- `Journey.Node.UpstreamDependencies.unblocked_when/1` - Helper function for defining unblock conditions
- `Journey.Node.UpstreamDependencies.unblocked_when/2` - Defines conditions using predicate trees with :and, :or, :not operations

## `Journey.Tools`
Utility functions for debugging, analysis, and visualization of Journey executions.

Functions:
- `Journey.Tools.abandon_computation/1` - Manually abandons a computation in :computing state, scheduling a retry if max_retries not exhausted
- `Journey.Tools.computation_state/2` - Returns the current state of a computation node (:not_set, :computing, :success, :failed, etc.)
- `Journey.Tools.computation_state_to_text/1` - Converts a computation state atom to human-readable text with visual symbols
- `Journey.Tools.computation_status_as_text/2` - Shows the status and dependencies for a single computation node
- `Journey.Tools.generate_mermaid_graph/2` - Generates a Mermaid diagram representation of a Journey graph
- `Journey.Tools.introspect/1` - Introspects an execution's current state with a human-readable text summary (primary debugging and introspection tool)
- `Journey.Tools.retry_computation/2` - Retries a failed computation by making previous attempts stale
- `Journey.Tools.summarize_as_data/1` - Generates structured data about an execution's current state
- `Journey.Tools.what_am_i_waiting_for/2` - Shows the status of upstream dependencies for a computation node

## `Journey.Insights.FlowAnalytics`
System-wide aggregate data about the state of executions for a particular graph. Business-focused analytics for understanding customer behavior.

Functions:
- `Journey.Insights.FlowAnalytics.flow_analytics/3` - Provides business-focused analytics for understanding customer behavior through Journey graphs
- `Journey.Insights.FlowAnalytics.to_text/1` - Formats flow analytics data as human-readable text output

## `Journey.Insights.Status`
System health and monitoring insights for Journey executions.

Functions:
- `Journey.Insights.Status.status/0` - Returns current system health for monitoring/alerting (:healthy/:unhealthy status, DB connectivity, graph statistics)
- `Journey.Insights.Status.to_text/1` - Formats status data as human-readable text output


## Example: `Journey.Examples.CreditCardApplication`
Demonstrates building a complete credit card application workflow using Journey.

See the full implementation: [`lib/journey/examples/credit_card_application.ex`](https://github.com/shipworthy/journey/blob/main/lib/journey/examples/credit_card_application.ex)

## Example: `Journey.Examples.UselessMachine`
Contains a simple example of building a "Useless Machine" using Journey - a reactive system that automatically turns itself off when turned on.

See the full implementation: [`lib/journey/examples/useless_machine.ex`](https://github.com/shipworthy/journey/blob/main/lib/journey/examples/useless_machine.ex)

