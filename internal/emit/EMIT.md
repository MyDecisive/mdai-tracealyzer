# Emit Subsystem Design

This document describes the `internal/emit` subsystem in more detail than the
top-level project design. Its purpose is to make the emission path implementable
before ingest, buffer, sweep, and topology are fully wired.

It is intentionally narrower than `requirements.md` and `design.md`:

- `requirements.md` defines the external contract.
- `design.md` places `emit` in the end-to-end pipeline.
- `EMIT.md` defines the internal flow, abstractions, and tradeoffs for the
  emitter itself.

## Purpose

The emitter accepts computed topology rows from the sweep/topology stage,
batches them, and writes them to GreptimeDB using the native Go ingester SDK.

The emitter must satisfy these core constraints from the requirements:

- rows are written to the `trace_topology` table in GreptimeDB
- rows follow the schema defined in `requirements.md` §5.2
- writes are batched
- write failures are retried with exponential backoff
- permanently failed rows are dropped, counted, and logged
- GreptimeDB problems must not stall the rest of the service indefinitely

## Placement In The Pipeline

The emitter is the final stage of the tracealyzer pipeline:

```text
Valkey -> sweep -> topology.Compute -> emit.Emit -> GreptimeDB
```

There is no separate buffering service between topology and emit. The queue
belongs inside the emitter implementation.

That means the call graph stays simple:

1. sweep finalizes a trace
2. topology computes one or more `RootMetrics` rows
3. topology or sweep passes those rows directly to `Emitter.Emit`
4. the emitter owns queueing, batching, retry, and GreptimeDB writes

## Why The Emitter Needs An Internal Queue

The emitter should use a bounded internal queue, implemented as a Go channel.

This is not a new architectural stage. It is an internal mechanism that
decouples:

- producer-side handoff: `Emitter.Emit(...)`
- consumer-side write path: batch flush + retry + GreptimeDB SDK write

This is needed because the write side may stall for reasons that should not
directly block the producer:

- GreptimeDB latency spikes
- short GreptimeDB outages
- retry backoff windows
- bursts of finalized traces from the sweep worker pool

Without a queue, `Emit` would either:

- perform writes synchronously, exposing topology/sweep workers to storage
  latency, or
- implement ad hoc concurrency elsewhere, which is harder to reason about

With a bounded queue:

- short disruptions are absorbed
- normal bursts are smoothed
- long outages degrade in a controlled way via drop-and-count behavior
- memory use remains bounded

## Non-Goals

The emitter does not:

- compute topology metrics
- reconstruct traces
- decide whether a trace is finalizable
- persist queued rows across process restarts
- guarantee delivery under prolonged downstream outage

Best-effort delivery is acceptable, but silent loss is not.

## High-Level Flow

### Normal path

```text
topology rows
    |
    v
Emitter.Emit(rows)
    |
    v
bounded internal queue
    |
    v
background flush loop
    |
    +-- flush on batch size
    |
    +-- flush on timer
    |
    +-- flush on Close()
    v
GreptimeDB writer
```

### Failure path

```text
GreptimeDB write fails
    |
    v
retry with exponential backoff
    |
    +-- success -> batch considered delivered
    |
    +-- retries exhausted -> batch dropped
                           -> topology_emissions_failed_total += batch size
                           -> structured error log with bounded trace_id list
```

### Queue saturation path

```text
Emit(rows)
    |
    +-- queue has capacity -> enqueue
    |
    +-- queue full -> drop rows immediately
                   -> topology_emissions_failed_total += dropped rows
                   -> structured warning/error log
```

## Public API Shape

The emitter should expose a very small package surface:

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

type Emitter interface {
    Emit(ctx context.Context, rows []RootMetrics) error
    Close(ctx context.Context) error
}
```

Notes:

- `Emit` means "accept for eventual emission", not "write synchronously now".
- `Close` means "stop intake, flush what is still buffered if possible, then
  shut down".
- `RootMetrics` may later move into a shared domain package once topology is
  implemented. For the first implementation it may live in `internal/emit` if
  that minimizes churn.

## Expected Emit Semantics

`Emit` should follow these rules:

- `Emit` with `nil` or empty rows returns `nil`
- `Emit` after close returns a stable error such as `ErrClosed`
- `Emit` should not block indefinitely
- `Emit` should try to enqueue quickly and return
- if the queue is full, rows are dropped immediately
- dropped rows must be counted and logged

The emitter is intentionally asynchronous. The caller should not assume that a
successful `Emit` call means the rows have already been written to GreptimeDB.

## Input Ownership

Because `Emit` is asynchronous, there is an ownership question around the input
slice. The current safe default is for `Emit` to clone the submitted rows before
placing them on the internal queue. That gives the emitter a stable snapshot and
protects it from caller-side mutation or slice reuse after `Emit` returns.

Without cloning, this common producer pattern would be unsafe:

```go
_ = emitter.Emit(ctx, rows)
rows = rows[:0]
rows = append(rows, nextRows...)
```

The append may reuse the same backing array, changing data that the background
emitter goroutine has not processed yet.

If profiling shows the copy is too expensive, cloning may be removed only if
the API contract becomes an explicit ownership-transfer contract:

```text
After calling Emit(ctx, rows), the caller must not mutate the slice elements or
reuse the slice backing array until the emitter has processed the rows.
```

That stricter contract is acceptable only because the expected callers are
internal sweep/topology components that we control. For a broader public API,
cloning is the safer default.

## Internal Components

The `internal/emit` package should separate concerns into a few focused pieces.

### 1. Front-door emitter

Responsible for:

- exposing `Emit` and `Close`
- owning the bounded queue
- starting and stopping the background loop
- validating package-level lifecycle state

### 2. Batch accumulator

Responsible for:

- gathering rows from the queue
- flushing on batch size
- flushing on timer
- flushing on shutdown

This logic should stay independent from the GreptimeDB SDK where possible.

### 3. Mapper

Responsible for converting `RootMetrics` into an internal row representation
that mirrors the GreptimeDB schema from `requirements.md` §5.2.

This should be a pure transformation layer.

### 4. Retrying write path

Responsible for:

- calling the underlying writer
- applying exponential backoff
- deciding when to give up
- recording failed-row metrics and logs on exhaustion

### 5. GreptimeDB adapter

Responsible for:

- holding the real SDK client
- converting the internal row batch into SDK structures
- issuing the actual `Write` call

This layer should be as thin as possible.

## Internal Abstractions

The central test seam should be a narrow writer interface.

```go
type writer interface {
    Write(ctx context.Context, batch writeBatch) error
    Close() error
}
```

The emitter should depend on `writer`, not directly on the GreptimeDB SDK
client. This gives us mockable tests while upstream phases are not yet built.

Recommended internal value types:

```go
type row struct {
    Timestamp       time.Time
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

type writeBatch struct {
    Table string
    Rows  []row
}
```

Recommended constructor shape:

```go
func New(
    cfg Config,
    logger *zap.Logger,
    metrics Metrics,
    w writer,
    now func() time.Time,
) Emitter
```

Where `Config` is an emitter-local runtime config:

```go
type Config struct {
    TableName      string
    Timeout        time.Duration
    MaxRetries     int
    InitialBackoff time.Duration

    BatchSize      int
    FlushInterval  time.Duration
    QueueCapacity  int
}
```

## Data Model Mapping

Each `RootMetrics` input becomes one row in the `trace_topology` table.

Schema mapping:

| Internal field | GreptimeDB column | Kind |
|---|---|---|
| `Timestamp` | `timestamp` | timestamp |
| `RootID` | `root_id` | tag |
| `TraceID` | `trace_id` | tag |
| `RootService` | `root_service` | field |
| `RootOperation` | `root_operation` | field |
| `Breadth` | `breadth` | field |
| `ServiceHopDepth` | `service_hop_depth` | field |
| `ServiceCount` | `service_count` | field |
| `OperationCount` | `operation_count` | field |
| `SpanCount` | `span_count` | field |
| `ErrorCount` | `error_count` | field |
| `RootDurationNS` | `root_duration_ns` | field |

Important notes:

- `root_id` is the complete `service.name::operation` identifier
- `trace_id` remains a tag in v1
- `timestamp` should represent finalization/emission time, not root span start
  time

## Timestamp Decision

The emitter should stamp rows at flush time, not at enqueue time.

Reasoning:

- the requirements define `timestamp` as trace finalization time
- the write-side batcher is the last stage before persistence
- stamping at flush time avoids stale timestamps for rows that sit in the queue
  briefly

This does mean rows in the same batch share a very similar timestamp, which is
acceptable for this use case.

## Queueing Model

The queue should be:

- bounded
- internal to the emitter
- best-effort

The queue should not be:

- unbounded
- visible to callers
- relied on for durable storage

Recommended v1 behavior:

- queue item type: `[]RootMetrics` or an internal batch item
- enqueue is non-blocking or very short-blocking
- on full queue, drop immediately

The exact queue capacity can start as an internal constant if we do not want to
expand `config.Config` yet. If we want full parity with the batching design from
day one, `queue_capacity`, `batch_size`, and `flush_interval` should eventually
be configurable.

## Concurrency Model

The emitter owns one queue per emitter instance. The queue is created when the
emitter is constructed, not each time `Emit` is called.

Multiple goroutines may safely call `Emit` on the same emitter instance. All of
those producer goroutines send into the same bounded channel. Go channels are
safe for concurrent sends, so this does not require an additional lock around
the send path beyond lifecycle checks such as "is the emitter closed?".

The first implementation uses one background consumer goroutine on the other
side of the channel. That goroutine is responsible for:

- reading queued rows
- accumulating batches
- flushing batches to GreptimeDB
- applying retry/drop behavior

This means GreptimeDB writes are serialized through one batching point. That is
intentional for v1 because it keeps batching, shutdown, retry, and metric
accounting simple. If one consumer cannot keep up, the bounded queue fills and
`Emit` returns `ErrQueueFull`; that is the explicit overload behavior.

If profiling later shows the single writer goroutine is the bottleneck, the
emitter can be extended with multiple writer workers. That would be a separate
design decision because it changes ordering, retry coordination, and shutdown
semantics.

## Batching Model

The emitter should batch rows for efficiency.

Flush triggers:

- row count reaches `BatchSize`
- `FlushInterval` elapses while there are buffered rows
- `Close` is called

The batcher should preserve row order within a flush. Cross-batch ordering does
not need stronger guarantees than FIFO queue consumption.

## Retry Model

On write failure, the emitter should retry using exponential backoff:

- initial interval = configured `InitialBackoff`
- multiplier = 2x
- stop after configured `MaxRetries`

Semantics:

- success after retries: batch is considered delivered
- retries exhausted: batch is dropped
- dropped rows increment `topology_emissions_failed_total` by row count
- error log includes a bounded subset of `trace_id`s

The retry loop should honor context cancellation during shutdown.

## Metrics

For the emitter phase, the key required metric is:

- `topology_emissions_failed_total`

This counter should increase for:

- batches dropped after retry exhaustion
- rows dropped because the queue is full

The metric must not increase for:

- batches that eventually succeed after retry
- rows successfully accepted and later written

The counter should count rows, not batches, since the requirement language is
trace-oriented and operator-facing.

## Logging

The emitter should use structured logs.

Suggested error log fields for a permanently failed batch:

- `table_name`
- `batch_size`
- `trace_ids` limited to a small bounded subset
- `max_retries`
- error details

Suggested log fields for queue saturation:

- `dropped_rows`
- `queue_capacity`
- bounded `trace_ids`

Success-path logging, if present, should stay at debug level.

## Lifecycle And Shutdown

`Close(ctx)` should:

1. stop accepting new rows
2. signal the background loop to stop reading new queue items
3. flush whatever is already buffered
4. stop retrying when `ctx` expires
5. close the underlying writer

If shutdown times out, the emitter may abandon undelivered rows. Those rows
must already be reflected in logs and metrics where applicable.

After `Close`, calls to `Emit` should fail fast with `ErrClosed`.

## Testing Strategy

Because ingest, buffer, sweep, and topology are not implemented yet, the
emitter should be designed around mock-friendly seams.

### Unit tests should cover

- mapping from `RootMetrics` to internal `row`
- batch flush on size
- batch flush on timer
- flush on `Close`
- retry success after transient failure
- permanent failure after retries exhausted
- queue-full drop behavior
- post-close `Emit` behavior
- logging on failure paths

### What should be mocked

- the internal `writer`
- time source where useful
- backoff timing if deterministic retry tests are needed

### What should stay real in tests

- batch accumulator logic
- mapping logic
- queue behavior
- emitter lifecycle

### Adapter tests

The real GreptimeDB adapter should have a smaller set of focused tests that
verify schema mapping into the SDK model. It should not re-test queueing,
batching, or retry behavior.

## Open Design Decisions

The following points should be settled during implementation:

### Where `RootMetrics` lives

Options:

- `internal/emit` for the first cut
- a shared domain package later when topology exists

### Whether batching knobs are public config yet

Current `config.Config` includes:

- endpoint
- database
- auth
- table name
- timeout
- max retries
- initial backoff

It does not yet include:

- batch size
- flush interval
- queue capacity

We can start with internal defaults or extend config before implementation.

### Queue-full return behavior

Two valid choices:

- return `nil` and rely on metrics/logs
- return a stable error such as `ErrQueueFull`

For this service, drop-and-count with a `nil` return is likely the cleaner
choice because callers should not synchronously retry emission.

## Recommended First Implementation Cut

Build the emitter in this order:

1. define `RootMetrics`, `Emitter`, and internal row/batch types
2. implement the pure mapping layer
3. implement the bounded-queue front door and flush loop
4. implement retry behavior against a mocked `writer`
5. add the real GreptimeDB SDK adapter
6. wire config and observability once the package behavior is stable

This yields a fully testable emitter even before the earlier pipeline stages
exist.
