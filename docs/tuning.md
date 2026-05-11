# Tuning

Operator guide to sizing tracealyzer for a given trace volume and freshness budget. All parameters are defined on `Config` (`internal/config/config.go`) and may be set through YAML fields or `TRACEALYZER_*` environment variables. Defaults below match `defaults()`.

## Pipeline

```
OTLP ─► ingest ─► buffer (Valkey) ─► sweeper ─► topology compute ─► emitter ─► GreptimeDB
                  └─ per-trace hash    └─ N workers                   └─ in-process pending batch
```

Three queues sit between stages: the per-trace Valkey hash, the sweeper's per-tick finalize work, and the emitter's pending batch. Tuning keeps all three drained without thrashing GreptimeDB or holding traces in Valkey past the freshness budget.

## Parameters

### Buffer (`buffer.*`)

| Parameter | Env | Default | Raising | Lowering |
|---|---|---|---|---|
| `quiet_period` | `TRACEALYZER_QUIET_PERIOD` | `60s` | Tolerates longer-tailed traces and reduces premature finalization (lower orphan rate). | Improves freshness at the cost of more partial-trace risk. |
| `max_ttl` | `TRACEALYZER_MAX_TTL` | `10m` | Covers slower upstream traces; increases Valkey memory use. | Bounds buffer footprint; raises `trigger="max_ttl"` and orphan rates. |
| `sweep_interval` | `TRACEALYZER_SWEEP_INTERVAL` | `5s` | Reduces Valkey scan QPS; raises finalization latency. | Finalizes sooner; raises Valkey scan QPS. |
| `sweep_worker_pool_size` | `TRACEALYZER_SWEEP_WORKER_POOL_SIZE` | `runtime.NumCPU()` | Clears larger finalize backlogs per tick. | Reduces CPU and Valkey pressure; allows backlog growth. |
| `valkey_operation_timeout` | `TRACEALYZER_VALKEY_OPERATION_TIMEOUT` | `10s` | Tolerates slower Valkey round trips; lengthens worst-case shutdown drain. | Improves shutdown liveness; raises the chance of an ambiguous Drain timeout under network stress (server processed `EXEC`, client read failed). |

### Emitter (`emitter.*`)

| Parameter | Env | Default | Raising | Lowering |
|---|---|---|---|---|
| `batch_size` | `TRACEALYZER_BATCH_SIZE` | `100` | Reduces GreptimeDB writes per row; increases per-row staleness. | Improves freshness; raises write QPS. |
| `flush_interval` | `TRACEALYZER_FLUSH_INTERVAL` | `1s` | Absorbs more rows per write under low volume. | Bounds row staleness; raises idle-flush QPS. |
| `queue_capacity` | `TRACEALYZER_QUEUE_CAPACITY` | `1024` | Absorbs downstream stalls; increases memory use. | Surfaces backpressure earlier; rows are rejected sooner. |
| `max_retries` | `TRACEALYZER_MAX_RETRIES` | `3` | Survives longer GreptimeDB outages. | Drops rows sooner on transient errors. |
| `initial_backoff` | `TRACEALYZER_INITIAL_BACKOFF` | `1s` | Spaces retries further under load. | Retries sooner; risks retry storms. |
| `timeout` | `TRACEALYZER_TIMEOUT` | `10s` | Tolerates slow GreptimeDB writes; lengthens stalls. | Cancels stuck writes earlier; causes avoidable retries. |

### Service (`service.*`)

| Parameter | Env | Default | Notes |
|---|---|---|---|
| `shutdown_grace` | `TRACEALYZER_SHUTDOWN_GRACE` | `30s` | Upper bound on orderly drain. Must exceed `flush_interval + timeout × (max_retries + 1)` *and* `valkey_operation_timeout` plus per-worker compute/emit overhead, or in-flight rows are dropped on rollout. |
| `log_level` | `TRACEALYZER_LOG_LEVEL` | `info` | `debug` is appropriate for incident triage; not recommended for steady-state at high QPS. |

## Profiles

### Low volume, freshness-first

Short windows across the pipeline.

```yaml
buffer:  { quiet_period: 15s, max_ttl: 2m, sweep_interval: 2s }
emitter: { batch_size: 25,    flush_interval: 500ms }
```

Trades GreptimeDB write QPS for sub-minute row latency. Monitor `topology_emissions_failed_total`; small batches amplify the impact of any write that fails after retry.

### High volume, batch-oriented

Wide windows and large batches with headroom for spikes.

```yaml
buffer:  { quiet_period: 90s, max_ttl: 15m, sweep_interval: 10s, sweep_worker_pool_size: 16 }
emitter: { batch_size: 500,   flush_interval: 5s, queue_capacity: 8192 }
```

Trades freshness for throughput. Scale `sweep_worker_pool_size` against finalize backlog rather than CPU count alone.

### Unstable downstream

Default cadence with extended retry budget and queue depth.

```yaml
emitter: { max_retries: 8, initial_backoff: 2s, queue_capacity: 16384, timeout: 20s }
service: { shutdown_grace: 5m }
```

Absorbs minute-scale GreptimeDB outages without dropping rows. `shutdown_grace` must exceed the worst-case retry chain.

## Diagnostics

| Symptom | Action |
|---|---|
| `topology_buffer_rejected_total{reason="overflow"}` rising | Adjust Valkey `maxmemory`; this is out of process and not a tracealyzer parameter. |
| `topology_buffer_rejected_total{reason="backend_error"}` rising | Investigate Valkey health; configuration changes will not help. |
| `topology_finalization_trigger_total{trigger="max_ttl"}` share rising | Raise `quiet_period` or `max_ttl`; traces are exceeding the quiet check. |
| `topology_compute_skipped_total{reason="no_root"}` rising | `max_ttl` is too short and traces are flushing before the root arrives, or upstream is truncating. Verify ingest before adjusting configuration. |
| `topology_orphan_spans_total` / `topology_orphan_bytes_total` rising | Same root cause as `no_root`. Raise `max_ttl` first, then investigate upstream. |
| `topology_root_id_collisions_total` non-zero | Two or more authentic roots in a single trace shared `(service, operation)`. The counter reports the number of rows merged away by the source-table primary key `(root_id, trace_id)`. The sweep WARN log carries `trace_id` and the merged-row count; query the source table for that `trace_id` to see the colliding `service::operation`. Investigate upstream: trace_id reuse in batch/queue code, 64-bit Datadog trace-id widening through the OTel `datadogreceiver`, or an SDK that emits `parent_id == 0` while a trace context is already active. |
| `topology_compute_duration_seconds` p99 climbing | Raise `sweep_worker_pool_size`. If CPU-bound, the trace shape is the underlying cause. |
| `topology_emissions_failed_total` rising | The counter is unlabeled; correlate with the `drop topology rows` warning logs and read the `reason` field. `write retries exhausted` or `write retries canceled` → raise `max_retries` and `initial_backoff` before adjusting `batch_size`; batch size does not address transient errors. `shutdown grace expired` (rises on rollouts) → raise `shutdown_grace` to cover the emitter retry envelope. `queue full` → raise `queue_capacity` or investigate writer throughput. `emitter closed` → late `Emit` after Shutdown, a lifecycle bug. |
| `topology_drain_errors_total` rising | Investigate Valkey; configuration changes will not help. |

## Constraints

- `max_ttl ≤ quiet_period` is rejected by `Validate()`. `max_ttl` is the cap, not the target.
- `quiet_period < sweep_interval` has no effect; the effective floor on finalization latency is `sweep_interval`.
- Raising `batch_size` without raising `queue_capacity` shifts the bottleneck to the queue.
- `flush_interval` is a freshness ceiling, not a throughput control. Lowering it raises GreptimeDB write QPS without increasing row throughput.
- `shutdown_grace` must exceed `flush_interval + timeout × (max_retries + 1)` *and* `valkey_operation_timeout` plus per-worker compute/emit overhead, or rollouts drop in-flight rows.
- `valkey_operation_timeout` should be less than `shutdown_grace` and large enough for normal Valkey round trips. Sweeper `Drain` is not cancelled by shutdown ctx; this is the bound that lets Shutdown return within grace. Lowering it improves shutdown liveness but increases the chance of an ambiguous Drain timeout under network stress.
- `sweep_worker_pool_size` defaults to `runtime.NumCPU()` of the pod, not the node. Set it explicitly when running under a CPU limit.
