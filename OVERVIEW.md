# Reference Guide

**Journey** is an Elixir library for building **persistent, reactive computation graphs** that manage data flows, executions, and background scheduling with PostgreSQL persistence. This reference guide provides a comprehensive overview of all Journey modules, functions, and capabilities. For tutorials and examples, see the [README](README.md).

## Workflow Scale

Journey handles **both small and large workflows** - from simple 2-3 node flows to complex 60+ node processes with no complexity penalty. Even basic 2-node workflows get production-grade persistence, error recovery, and audit trails.

## Overview of Modules

### 1. **Journey**

Main API for defining graphs and managing executions.

- **Graph Management**: [`new_graph/4`](Journey.html#new_graph/4) - Define computation workflows
- **Execution Lifecycle**: [`start_execution/1`](Journey.html#start_execution/1), [`load/2`](Journey.html#load/2), [`archive/1`](Journey.html#archive/1), [`unarchive/1`](Journey.html#unarchive/1)
- **Value Operations**: [`set_value/3`](Journey.html#set_value/3), [`get_value/3`](Journey.html#get_value/3), [`unset_value/2`](Journey.html#unset_value/2)
- **State Inspection**: [`values/2`](Journey.html#values/2), [`values_all/2`](Journey.html#values_all/2), [`history/1`](Journey.html#history/1)
- **Query/Analytics**: [`list_executions/1`](Journey.html#list_executions/1) with powerful filtering and pagination
- **Background Processing**: Automatic scheduling and computation management

### 2. **Journey.Node**

Node types used for defining graphs.

- **[`input/1`](Journey.Node.html#input/1)** - Accept user-provided data
- **[`compute/4`](Journey.Node.html#compute/4)** - Self-computing nodes with dependency management
- **[`mutate/4`](Journey.Node.html#mutate/4)** - Modify values of other nodes (PII redaction, cleanup)
- **[`archive/3`](Journey.Node.html#archive/3)** - Auto-archive executions when triggered
- **[`schedule_once/4`](Journey.Node.html#schedule_once/4)** - One-time scheduled execution at specific time
- **[`schedule_recurring/4`](Journey.Node.html#schedule_recurring/4)** - Recurring scheduled executions

### 3. **Journey.Node.Conditions**

Helpers for defining conditional dependencies in graphs.

- **[`provided?/1`](Journey.Node.Conditions.html#provided?/1)** - Check if node has value (including scheduled time check)
- **[`true?/1`](Journey.Node.Conditions.html#true?/1)** - Check if node value is `true`
- **[`false?/1`](Journey.Node.Conditions.html#false?/1)** - Check if node value is `false`

### 4. **Journey.Node.UpstreamDependencies**

Functions for defining complex dependencies in a graph.

- **[`unblocked_when/2`](Journey.Node.UpstreamDependencies.html#unblocked_when/2)** - Define complex conditional dependencies
- Supports `:and`, `:or`, `:not` logical operations
- Nested predicate trees for sophisticated workflow control

### 5. **Journey.Tools**

Utility functions for troubleshooting and introspection.

- **State Inspection**: [`computation_state/2`](Journey.Tools.html#computation_state/2), [`computation_state_to_text/1`](Journey.Tools.html#computation_state_to_text/1)
- **Debugging**: [`what_am_i_waiting_for/2`](Journey.Tools.html#what_am_i_waiting_for/2), [`computation_status_as_text/2`](Journey.Tools.html#computation_status_as_text/2)
- **Visualization**: [`generate_mermaid_graph/2`](Journey.Tools.html#generate_mermaid_graph/2) - Create Mermaid diagrams
- **Execution Analysis**: [`summarize_as_text/1`](Journey.Tools.html#summarize_as_text/1), [`summarize_as_data/1`](Journey.Tools.html#summarize_as_data/1)
- **Error Recovery**: [`retry_computation/2`](Journey.Tools.html#retry_computation/2)

### 6. **Journey.Insights**

Analytics and monitoring capabilities for graph performance analysis.

#### **Journey.Insights.FlowAnalytics**
- **[`flow_analytics/3`](Journey.Insights.FlowAnalytics.html#flow_analytics/3)** - Comprehensive graph analytics with execution stats and per-node customer journey metrics
- **[`to_text/1`](Journey.Insights.FlowAnalytics.html#to_text/1)** - Format analytics data as human-readable text

#### **Journey.Insights.Status**
- **[`status/0`](Journey.Insights.Status.html#status/0)** - System health monitoring with database connectivity and per-graph statistics
- **[`to_text/1`](Journey.Insights.Status.html#to_text/1)** - Format status data as human-readable text

### 7. **[Journey.Examples.CreditCardApplication](Journey.Examples.CreditCardApplication.html)**

An example of a graph with rich functionality.

Demonstrates a complete credit card application workflow with:
- Personal information collection
- Credit score checking and approval
- Scheduled reminders
- PII redaction after processing
- Card issuance and notifications
- Auto-archiving when complete

### 8. **[UselessMachine](UselessMachine.html)**

A simple example demonstrating Journey's mutate functionality.

Shows how to build a "Useless Machine" that automatically turns itself off when switched on, illustrating reactive node behavior and mutate operations.

### 9. **[Migration Strategy](migration_strategy.html)**

Guidelines for evolving graphs.

- **Breaking vs Non-breaking Changes** guidelines
- **Version Management** - when to create new graph versions
- **Execution Continuity** - old executions continue with old graph definitions
- **Deployment Strategies** for graph updates

## Key Features & Capabilities

### Persistence & Resilience
- **PostgreSQL Backend** - All state persisted to database
- **Crash Recovery** - Executions survive application and infrastructure restarts and crashes
- **Background Processing** - Automatic scheduling and computation
- **Revision Tracking** - Complete audit trail of all changes

### Orchestration of Computations

Nodes are computed when their upstream dependencies are satisfied.

- **Simple Dependencies** - List of required nodes `[:a, :b]`
- **Conditional Dependencies** - Predicate functions on node values
- **Logical Operations** - `:and`, `:or`, `:not` for complex conditions
- **Structured Conditions** - Nested logic trees
- **Temporal Dependencies** - Schedule-based unblocking

### Error Handling & Recovery
- **Auto-retry** - Failed computations retry up to `max_retries` times
- **Timeouts** - Computations abandoned after `abandon_after_seconds`
- **Graceful Failure** - `{:error, reason}` results tracked in execution
- **Manual Retry** - [`Journey.Tools.retry_computation/2`](Journey.Tools.html#retry_computation/2)

### Performance & Scalability
- **Database-level Filtering** - Efficient queries in [`list_executions/1`](Journey.html#list_executions/1)
- **Pagination Support** - `limit` and `offset` for large result sets
- **Background Sweeps** - Non-blocking scheduled task processing
- **Lazy Loading** - Optional `preload: false` for performance

### Developer Experience
- **Mermaid Visualization** - Auto-generate workflow diagrams
- **Rich Debugging** - Detailed state inspection and analysis
- **Migration Guidance** - Clear strategies for evolving graphs
- **Comprehensive Examples** - Real-world workflow demonstrations
- **Testing Support** - Background sweep helpers for test environments

### Security & Data Management
- **PII Handling** - Mutate nodes for sensitive data redaction
- **Archive System** - Hide completed executions while preserving data
- **Value Type Safety** - Automatic JSON serialization/deserialization
- **Audit Trail** - Complete history of all computations and value changes

## Use Cases
Journey scales from simple to complex workflows. Here are some examples.

**Simple Workflows (2-3 nodes):**
- Email verification (email → verify → confirmed)
- Basic approvals (request → review → approve/reject)
- Simple notifications

**Complex Workflows (10+ nodes):**
- User onboarding with conditional steps
- Financial applications (credit processing, loan approval)
- Content generation pipelines
- ETL and data transformation flows
- Multi-step forms with conditional logic
- Scheduled task orchestration

## Quick Start References

**Setup**: [Installation and Configuration](README.html#installation-and-configuration) | [Database Setup](README.html#installation-and-configuration) | [Graph Registration](README.html#installation-and-configuration)

**Basic Flow**: [`Journey.new_graph/4`](Journey.html#new_graph/4) → [`Journey.start_execution/1`](Journey.html#start_execution/1) → [`Journey.set_value/3`](Journey.html#set_value/3) → [`Journey.get_value/3`](Journey.html#get_value/3)

**Debugging**: [`Journey.Tools`](Journey.Tools.html) utilities | [`generate_mermaid_graph/2`](Journey.Tools.html#generate_mermaid_graph/2) for visualization

**Examples**: [Basic Concepts](README.html#basic-concepts) | [Step-by-Step Tutorial](README.html#step-by-step) | [`Journey.Examples.CreditCardApplication`](Journey.Examples.CreditCardApplication.html) for complete implementation

## External Examples

**[Journey Demo](https://github.com/shipworthy/journey-demo)** - A comprehensive real-world example application demonstrating Journey's capabilities in a complete Phoenix web application
