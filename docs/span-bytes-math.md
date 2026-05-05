# Span bytes per root

`span_bytes_total` is the per-root proxy for Datadog APM ingestion bytes. It is computed in three stages: per-span attribution at ingest, per-root summation at topology compute, and per-window aggregation in the GreptimeDB flow.

## Per-span attribution (ingest)

OTLP delivers spans wrapped in a `ResourceSpans` envelope that carries the resource attributes and scope spans containers. Wire-format byte cost is therefore split between the spans themselves and the envelope around them. Tracealyzer attributes the envelope cost back onto the spans so that summing per-span bytes recovers the full on-the-wire cost of the `ResourceSpans` message.

For one `ResourceSpans` `rs` containing spans `s_1 … s_N`:

```
size_i      = proto.Size(s_i)                                    // raw span bytes
span_sum    = Σ size_i
overhead    = max(proto.Size(rs) - span_sum, 0)                  // resource + scope framing
base, rem   = overhead / N, overhead % N
attributed_i = size_i + base + (1 if i < rem else 0)             // i = 0 … N-1
```

Each span's `attributed_i` is stored as `SpanRecord.SizeBytes` (`internal/ingest/normalize.go:65`).

Properties:

- **Conservation.** `Σ attributed_i == proto.Size(rs)` whenever `proto.Size(rs) ≥ span_sum`. Sub-message sizing in `google.golang.org/protobuf` is monotonic, so the clamp only applies to degenerate input (for example a hand-crafted `rs` with zero spans pre-filtered, or a malformed message). Under normal traffic `overhead ≥ 0` holds.
- **Even split.** Distributing `rem` extra bytes to the first `rem` spans keeps each `attributed_i` within 1 byte of `size_i + overhead/N` and avoids floating-point rounding drift.
- **Stable order.** The remainder is distributed in the order spans appear in the `ScopeSpans` slices, making attribution deterministic for a given `ResourceSpans` payload.
- **Nil entries are skipped.** Nil scopes and nil spans are skipped before sizing and do not receive overhead.
- **Malformed spans are counted.** Sizing happens before OTLP validation (`toRecord`), so a span dropped later for a bad trace or span ID still consumed bytes on the wire and still receives its share of overhead. The bytes are not double-counted because the dropped span never reaches the buffer.

## Per-root summation (topology compute)

When a trace is finalized, `topology.Compute` walks each root's subtree depth-first and sums `SizeBytes` into `rootAccumulator.spanBytesTotal` (`internal/topology/topology.go:161`). The accumulator is reset per root, so multi-root traces produce one `span_bytes_total` per root covering only that root's reachable spans. Spans unreachable from any root are excluded from every root's total and are reported separately as orphan count.

The per-root row written to `trace_root_topology` therefore satisfies:

```
row.span_bytes_total = Σ SpanRecord.SizeBytes over spans reachable from root
```

## Per-window aggregation (flow)

The `trace_root_topology_1m_flow` continuous flow groups source rows by `(root_id, date_bin('1 minute', timestamp))` and emits `sum(span_bytes_total)` into `trace_root_topology_1m.span_bytes_total` (`internal/schema/schema.go:153`). One minute's value for a `root_id` is the sum of attributed OTLP bytes across every trace whose root matched that `root_id` and finalized in that window.

## Non-goals

- **Not compressed wire bytes.** `proto.Size` returns the uncompressed protobuf encoding. Gzip on the OTLP/HTTP path reduces transport bytes but not the metric. This matches Datadog APM billing, which is based on uncompressed bytes.
- **Not export-format bytes.** Bytes are measured against the OTLP ingress shape, not the format an exporter would later produce.
- **Not a per-span billing source of truth.** Overhead is apportioned evenly rather than weighted by span size: a 100 B span and a 10 KB span in the same `ResourceSpans` receive equal shares of the resource and scope framing. The total per `ResourceSpans` is exact; the split within it is a uniform attribution choice.
