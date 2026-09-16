# Tuning

Operator guide to sizing tracealyzer for a trace volume, a freshness budget, and a rollout drop budget. Parameters are fields on `Config` (`internal/config/config.go`) and are set through the Helm chart's `config.*` values. An environment variable overrides a YAML field; the accepted name is the section-prefixed form shown below, and the bare tag (`QUIET_PERIOD`) resolves to the same field.

## Pipeline

```
OTLP ─► ingest ─► buffer (Valkey) ─► sweeper ─► topology compute ─► emitter ─► GreptimeDB
```

Spans accumulate per trace in Valkey until the trace is quiet for `quiet_period` or has aged past `max_ttl`. The sweeper finalizes those traces every `sweep_interval`, computes topology rows, and hands them to the emitter, which writes to GreptimeDB in batches with retries.

## Parameters

`Default` is the built-in value; `Chart` is what a Helm install runs.

| Parameter | Env | Default | Chart | Meaning |
|---|---|---|---|---|
| `buffer.quiet_period` | `BUFFER_QUIET_PERIOD` | `60s` | `60s` | Idle time after the last span before a trace is finalized. Values below `sweep_interval` have no effect. |
| `buffer.max_ttl` | `BUFFER_MAX_TTL` | `10m` | `10m` | Age at which a trace is finalized regardless of activity. Must exceed `quiet_period`. |
| `buffer.sweep_interval` | `BUFFER_SWEEP_INTERVAL` | `5s` | `5s` | Finalization tick; the floor on finalization latency and the Valkey scan rate. |
| `buffer.sweep_worker_pool_size` | `BUFFER_SWEEP_WORKER_POOL_SIZE` | `NumCPU` | `NumCPU` | Parallel finalizations per tick. `NumCPU` is the pod's, not the node's; set explicitly under a CPU limit. |
| `buffer.valkey_operation_timeout` | `BUFFER_VALKEY_OPERATION_TIMEOUT` | `10s` | `10s` | Per-command Valkey deadline; bounds the sweeper's shutdown drain. |
| `emitter.batch_size` | `EMITTER_BATCH_SIZE` | `100` | `100` | Rows per GreptimeDB write. |
| `emitter.flush_interval` | `EMITTER_FLUSH_INTERVAL` | `1s` | `1s` | Maximum time a row waits for a full batch. |
| `emitter.queue_capacity` | `EMITTER_QUEUE_CAPACITY` | `1024` | `1000` | Rows buffered ahead of the writer; rows beyond it are dropped. |
| `emitter.max_retries` | `EMITTER_MAX_RETRIES` | `3` | `5` | Retries per batch after the first attempt. |
| `emitter.initial_backoff` | `EMITTER_INITIAL_BACKOFF` | `1s` | `10s` | First retry delay; doubles per retry, capped at `60s`. |
| `emitter.timeout` | `EMITTER_TIMEOUT` | `10s` | `10s` | Deadline per write attempt. |
| `service.shutdown_grace` | `SERVICE_SHUTDOWN_GRACE` | `30s` | `30s` | Deadline for the whole shutdown sequence; see Shutdown. |
| `service.log_level` | `SERVICE_LOG_LEVEL` | `info` | `info` | `debug` is for incident triage, not steady state. |

## Shutdown

On SIGTERM, components stop in reverse start order under a single `shutdown_grace` deadline: ingest servers, then the sweeper, then the emitter. The sweeper finishes its in-flight tick, bounded by `valkey_operation_timeout`. The emitter finishes the batch it is writing, then flushes queued rows in `batch_size` chunks. Every batch, in flight or queued, carries its own retry envelope:

```
envelope = timeout × (max_retries + 1) + Σ min(initial_backoff × 2ⁿ, 60s)   for n = 0 … max_retries − 1
```

The retry library stops the chain at 15 minutes of elapsed time regardless of the parameters.

`shutdown_grace` covers `valkey_operation_timeout` plus one envelope for the in-flight batch; each queued chunk to be saved during a downstream outage needs another envelope. `terminationGracePeriodSeconds` exceeds `shutdown_grace`, since the kubelet sends SIGKILL at the pod grace period regardless of drain progress.

Rows that miss the deadline are counted in `topology_emissions_failed_total` and logged under `drop topology rows` with reason `write retries canceled` (a batch mid-retry) or `shutdown grace expired` (rows still queued).

## Profiles

Freshness-first, low volume. Confirms when `topology_finalization_trigger_total{trigger="max_ttl"}` stays a small share.

```yaml
buffer:  { quiet_period: 15s, max_ttl: 2m, sweep_interval: 2s }
emitter: { batch_size: 25,    flush_interval: 500ms }
```

Throughput-first, high volume. Confirms when `topology_compute_duration_seconds` p99 and `topology_buffer_rejected_total` stay flat.

```yaml
buffer:  { quiet_period: 90s, max_ttl: 15m, sweep_interval: 10s, sweep_worker_pool_size: 16 }
emitter: { batch_size: 500,   flush_interval: 5s, queue_capacity: 8192 }
```

Unstable downstream. The envelope is `9 × 20s + (2+4+8+16+32+60+60+60)s = 7m2s`, so the grace covers one in-flight batch with margin for the sweeper drain. Confirms when `topology_emissions_failed_total` stays flat through GreptimeDB outages.

```yaml
emitter: { max_retries: 8, initial_backoff: 2s, queue_capacity: 16384, timeout: 20s }
service: { shutdown_grace: 8m }   # terminationGracePeriodSeconds ≥ 480
```

## Diagnostics

| Symptom | Action |
|---|---|
| `topology_buffer_rejected_total{reason="overflow"}` rising | Raise Valkey `maxmemory`; not a tracealyzer parameter. |
| `topology_buffer_rejected_total{reason="backend_error"}` or `topology_drain_errors_total` rising | Investigate Valkey health. |
| `topology_finalization_trigger_total{trigger="max_ttl"}` share rising | Raise `quiet_period` or `max_ttl`. |
| `topology_compute_skipped_total{reason="no_root"}` or `topology_orphan_spans_total` rising | Raise `max_ttl`; if unchanged, upstream is truncating traces. |
| `topology_root_id_collisions_total` non-zero | Multiple roots share `(service, operation)` in one trace; the sweep WARN log carries `trace_id`. Investigate upstream trace-id reuse. |
| `topology_compute_duration_seconds` p99 climbing | Raise `sweep_worker_pool_size`. |
| `topology_emissions_failed_total` rising | Read the `reason` field of the `drop topology rows` log. `write retries exhausted`: raise `max_retries` or `initial_backoff`. `queue full`: raise `queue_capacity`. `shutdown grace expired` or `write retries canceled` on rollouts: raise `shutdown_grace` per Shutdown. |
