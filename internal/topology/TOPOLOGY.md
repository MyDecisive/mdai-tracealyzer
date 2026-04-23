# Topology Subsystem Design

This document describes the high-level design for `internal/topology`: the pure
trace reconstruction and metric-computation stage.

It is intentionally focused on topology logic only. Ingest, buffering,
finalization sweep, and GreptimeDB emission are covered by the broader
`requirements.md`, `design.md`, and `internal/emit/EMIT.md` documents.

## Purpose

The topology subsystem takes the finalized spans for one trace and computes the
per-root metrics that will later be emitted to GreptimeDB.

Input:

- one trace ID
- all buffered spans drained for that trace

Output:

- zero or more root metric rows
- orphan span count

The package must stay pure:

- no Valkey
- no GreptimeDB
- no OTLP SDK/protobuf types
- no goroutines
- no logging
- no Prometheus registry

This keeps the algorithm easy to unit test with fixture traces.

## Data Flow

End-to-end pipeline context:

```text
OTLP ingest
  -> buffer.Put(SpanRecord)
  -> sweep detects finalizable trace
  -> buffer.Drain(trace_id)
  -> topology.Compute(trace_id, spans)
  -> emit.Emit([]RootMetrics)
```

Topology owns only this segment:

```text
map[span_id]Span
  -> identify authentic roots
  -> build parent/children index
  -> walk root subtrees
  -> compute RootMetrics
  -> count orphans
```

## Logical Blocks

### Input Model

Topology should define its own span input type. It should be close to the
buffer-drain representation, but independent from Valkey encoding and OTLP
types.

Expected shape:

```go
type Span struct {
    SpanID       [8]byte
    ParentSpanID [8]byte
    Service      string
    Name         string
    Kind         int32
    StartTimeNs  int64
    EndTimeNs    int64
    StatusError  bool
    OpAttrs      map[string]string
}
```

`OpAttrs` contains only the low-cardinality attributes needed for operation
derivation:

- `http.request.method`
- `http.route`
- `rpc.service`
- `rpc.method`
- `messaging.operation.type`
- `messaging.destination.name`

### Output Model

`RootMetrics` lives in `internal/topology`. It is the domain output of
topology analysis and the input contract consumed by `internal/emit`.

Reasoning: topology owns the metric semantics. Emission owns delivery and
GreptimeDB mapping. Having `emit` depend on `topology.RootMetrics` keeps the
dependency direction aligned with the data flow and avoids putting domain types
inside a persistence package.

Current skeleton:

```go
type RootMetrics struct {
    RootID          string
    TraceID         string
    RootService     string
    RootOperation   string
    Breadth         int32
    ServiceHopDepth int32
    ServiceCount    int32
    OperationCount  int32
    SpanCount       int32
    ErrorCount      int32
    RootDurationNS  int64
}
```

### Root Detection

A root span is a span whose `ParentSpanID` is the zero `[8]byte` value.

Only authentic roots are valid traversal starting points. A span with a
non-zero parent ID whose parent is missing is not promoted to root.

This is central to the v1 contract:

- missing-parent spans are dropped
- their disconnected subtrees are not emitted
- every dropped span is counted as an orphan

### Child Index

Topology should build:

```go
childrenByParent map[[8]byte][][8]byte
roots            [][8]byte
```

Construction pass:

1. Iterate all spans.
2. If `ParentSpanID` is zero, append to `roots`.
3. If parent exists in the span map, append child ID to
   `childrenByParent[parentID]`.
4. If parent does not exist, do not link the span anywhere.

This makes disconnected spans unreachable from authentic roots and therefore
eligible for orphan counting.

### Subtree Walker

Each root subtree should be walked iteratively with an explicit stack, not
recursively.

Reasons:

- traces can be very deep
- recursion risks goroutine stack growth or overflow on pathological traces
- explicit stack makes service-hop path state easier to carry

Each stack frame should carry:

```go
type frame struct {
    spanID   [8]byte
    depth    int32
    service  string
}
```

The exact fields can vary, but the walker must carry enough path-local state to
compute service-hop depth correctly.

## Metrics Computed

For each authentic root subtree:

| Metric | Source |
|---|---|
| `root_id` | root service + derived root operation |
| `trace_id` | input trace ID as 32-char lowercase hex |
| `root_service` | root span service |
| `root_operation` | operation derived from root span only |
| `breadth` | max direct child count of any span in subtree |
| `service_hop_depth` | max service transition depth across any root-to-leaf path |
| `service_count` | unique service names in subtree |
| `operation_count` | unique `(service, operation)` pairs in subtree |
| `span_count` | number of visited spans in subtree |
| `error_count` | visited spans with `StatusError == true` |
| `root_duration_ns` | root `EndTimeNs - StartTimeNs` |

Counters should use `int32` to map cleanly to the GreptimeDB schema. Duration
stays `int64`.

## Root Operation Derivation

Only the root span determines `root_operation`.

Implementation note: the code should use an ordered list of small operation
derivers rather than one growing conditional chain. That keeps precedence
explicit and makes new operation families additive.

Precedence:

1. HTTP: if `http.request.method` exists
   - if `http.route` exists: `METHOD route`
   - otherwise: root span name
2. RPC/gRPC: if `rpc.service` and `rpc.method` exist
   - `service/method`
3. Messaging: if `messaging.operation.type` and
   `messaging.destination.name` exist
   - `operation destination`
4. Fallback:
   - span name if non-empty
   - otherwise `unnamed_operation`

Suggested shape:

```go
type operationDeriver interface {
    Derive(span Span) (operation string, ok bool)
}
```

V1 derivers are:

- HTTP
- RPC/gRPC
- Messaging

Later kinds such as database can be added by appending a new deriver at the
appropriate point in the precedence order. This does not need to be
config-pluggable in v1.

The operation must not use high-cardinality attributes such as concrete URLs,
resource IDs, user IDs, order IDs, or `dd.span.Resource`.

## Orphan Handling

Topology should count any span not visited from an authentic root as an orphan.

This includes:

- spans whose parent is missing
- descendants of missing-parent spans
- spans in disconnected cycles
- traces with no authentic root at all

For a trace with no authentic root:

- return zero metric rows
- return `orphanCount == len(spans)`

The topology package should not increment Prometheus counters itself. It should
return `orphanCount`; sweep or composition should update
`topology_orphan_spans_total`.

## Concurrency Decision

`internal/topology` itself should not contain concurrency.

Topology computation is CPU-bound and pure. The concurrency boundary belongs in
the sweep/finalization layer, not in the algorithm package.

Recommended model:

- sweep scans finalizable trace IDs on a single ticker loop
- sweep uses a bounded worker pool to process traces in parallel
- each worker drains one trace, calls `topology.Compute`, and passes rows to
  the emitter
- `topology.Compute` processes one trace synchronously

Why not add goroutines inside topology?

- One trace is the natural unit of isolation.
- Per-trace parallelism adds overhead and complexity for little benefit at the
  expected trace size.
- The algorithm needs shared per-subtree sets and path-local state; parallel
  subtree walking makes orphan counting and metric aggregation easier to get
  wrong.
- Sweep-level parallelism already scales across traces, which is the expected
  workload shape.

If profiling later shows large single traces dominate compute time, we can
revisit per-root parallelism. That should be a measured optimization, not the
default design.

## Algorithm Sketch

Recommended `Compute` shape:

```go
func Compute(traceID [16]byte, spans map[[8]byte]Span) (roots []RootMetrics, orphanCount int)
```

High-level steps:

1. Return zero rows and zero orphans for empty input.
2. Build `childrenByParent` and `roots`.
3. Create a `visited` set.
4. For each root:
   - walk its reachable subtree with an explicit stack
   - mark every visited span
   - update breadth, service-hop depth, service set, operation set, span count,
     error count
   - compute root duration and root operation
   - append one `RootMetrics`
5. After all roots, `orphanCount = len(spans) - len(visited)`.

## Algorithm Comments And Concerns

### Multi-root Policy

A well-formed trace is expected to have one authentic root, but the span data
model does not make multiple roots impossible. A single `trace_id` can contain
more than one span with an empty zero `ParentSpanID`, usually because of
instrumentation bugs, partial ingestion, exporter behavior, or trace-stitching
issues.

V1 uses a tolerant forest policy:

- keep `Compute` returning `[]RootMetrics`
- treat every zero-parent span as an authentic root
- compute one `RootMetrics` row per authentic root
- do not merge separate root subtrees
- do not arbitrarily choose one root and drop the rest
- count orphans only as spans unreachable from any authentic root

Multiple roots for one trace ID are anomalous but valid input. Topology should
return deterministic rows for them. The sweep or composition layer can report a
counter such as `topology_multi_root_traces_total`; the pure topology package
should not log or increment Prometheus metrics directly.

### Operation Count Definition

Requirements define `operation_count` as unique `(service.name, operation)`
pairs in the subtree, where operation is computed per §4.2.

However, the current buffered attributes are intentionally stored only for root
spans. Child spans do not carry `OpAttrs`.

That creates an ambiguity:

- for root spans, operation derivation can use HTTP/RPC/messaging attributes
- for child spans, operation likely falls back to span name

Implementation should make this explicit:

- root operation uses root `OpAttrs`
- child operation falls back to `span.Name` unless child operation attributes
  are later retained

If precise child operation classification matters, ingest/buffer must preserve
operation attributes for non-root spans too. That is a memory/cardinality
tradeoff.

### Service Hop Depth Off-by-One

The requirements say the root itself counts as the first service, and the count
increments when service changes from parent to child.

Recommended interpretation:

- root starts at depth `1`
- same-service child keeps current depth
- different-service child increments depth

So a single-span trace has `service_hop_depth == 1`.

Tests should lock this down because it is an easy off-by-one source.

### Breadth Definition

`breadth` is maximum direct child count of any one span in the visited subtree.

It is not:

- total number of spans at the widest tree level
- number of unique services
- number of root children only

Tests should include a case where a non-root span has the widest fan-out.

### Cycles

Real span trees should not contain cycles, but bad input can.

Use `visited` during walking to avoid infinite loops. If a cycle is reachable
from a root, each span should be counted once. If a cycle is disconnected from
all roots, its spans are orphans.

### Duplicate Span IDs

The input is a map keyed by span ID, so duplicates are already collapsed before
topology sees them. Last-write-wins behavior belongs to the buffer layer, not
topology.

### Duration Sanity

`root_duration_ns = EndTimeNs - StartTimeNs`.

If timestamps are malformed and this becomes negative, topology has two
choices:

- preserve the negative value to expose bad upstream data
- clamp to zero

Current requirements do not specify clamping. Recommendation: preserve the raw
difference and add a test documenting the behavior.

### Deterministic Tests

Map iteration order is random. Tests should not rely on output ordering unless
`Compute` sorts root IDs before walking.

Recommendation:

- sort roots by span ID before computing output
- sort children per parent by span ID when deterministic traversal matters

This makes fixture tests stable and output predictable.

## Test Plan

Unit tests should be table-driven and use hand-built fixtures.

Required fixtures:

- simple chain
- basic fan-out
- rollback saga with three-way fan-out

Additional edge cases:

- empty input
- single-span trace
- all orphan trace
- missing parent with descendants
- multi-root trace
- reachable cycle
- unnamed root span
- HTTP root with and without route
- RPC root
- messaging root
- negative root duration
- non-root widest fan-out

Assertions should cover:

- every metric field
- orphan count
- root operation derivation
- deterministic output ordering

## Implementation Boundary

Topology should expose only the minimal API needed by sweep:

```go
package topology

func Compute(traceID [16]byte, spans map[[8]byte]Span) ([]RootMetrics, int)
```

Supporting helpers should stay unexported unless tests or other packages need
them directly.

`internal/emit` should import `internal/topology` for the row type. Topology
must not import `emit`.
