# mdai-tracealyzer

Trace topology service — computes structural metrics from OTLP traces and writes them to GreptimeDB for tail-sampling decisions.

## Overview

Tracealyzer ingests OTLP spans (gRPC on `4317`, HTTP on `4318`), buffers them per trace in Valkey, and — once a trace is quiet for long enough or reaches its max TTL — computes topology metrics (breadth, service-hop depth, service/operation/span/error counts, root duration, attributed OTLP span bytes) and writes one row per root to the `trace_root_topology` table in GreptimeDB. A continuous Greptime FLOW (`trace_root_topology_1m_flow`) pre-aggregates those rows into `trace_root_topology_1m`, which holds 1-minute UDDSketches for breadth, service-hop depth, and root duration alongside `trace_count`, `error_count_total`, `span_count_total`, and `span_bytes_total`, keyed by `root_id` within each 1-minute `time_window`. Dashboards consume the raw rows and the 1-minute sketches to surface traces worth keeping for tail-sampling decisions; `span_bytes_total` is the proxy for Datadog APM ingestion bytes per root.

The service does not make the sampling decision itself, store full trace data, render the dashboard, or alert. Span-link handling and multi-root traces are deferred past v1.

## Running the test-stand demo

The `test-stand/` tree exercises the upstream side — a Datadog agent plus several demo microservices that emit realistic traces. The flow is: demo services → in-cluster Datadog agent → OTel collector gateway → tracealyzer. Everything runs in the same kind cluster as tracealyzer itself, so traces flow end-to-end without any port-forward bridging.

Every demo Deployment (catalog, checkout, gateway, payments, inventory-http, inventory-grpc, notifier) runs the same `demo-svc` binary under a different `DEMO_ROLE`. Adding a new role means a new `cmd/demo-svc/role_<name>.go` plus a Deployment in `test-stand/deployment/demo-services.yaml` — no new module to wire.

### One-time setup

```sh
# Provision the Datadog API-key Secret in $(NAMESPACE) — required by demo-apply.
# Name is overridable via DEMO_DATADOG_SECRET_NAME; key must be `api-key`.
kubectl -n mdai create secret generic test-vv-integration-secret \
    --from-literal=api-key=<DD_API_KEY>

# Deploy the OTel collector gateway to the kind cluster.
make demo-deploy-gateway

# Build and deploy tracealyzer to the kind cluster.
make deploy-local
```

### Forwarder modes

The setup above wires the demo's Datadog agent at the in-project sample OTel Collector (**Mode A**). For **Mode B**, where the agent points at an existing in-cluster endpoint (e.g. an `mdai-envoy` Service), skip `demo-deploy-gateway` and use `make demo-apply-external DEMO_DATADOG_FORWARDER_URL=…` in place of `make demo-up`. See [`test-stand/deployment/README.md`](test-stand/deployment/README.md#forwarder-modes) for details.

### Per-session

```sh
# Build demo + load-generator images, kind-load them, apply the stack.
make demo-up

# Emit a single scenario (one-shot Pod, output streams back).
make demo-emit DEMO_SCENARIO=browse

# Optional: tail tracealyzer logs and metrics from elsewhere in the same cluster.
make logs
make metrics-forward       # then: curl localhost:9090/metrics
```

To poke the gateway by hand instead of via `make demo-emit`:

```sh
make demo-app-port-forward # service/gateway-api 8081:8080
curl -s 'http://localhost:8081/wide?scenario=wide'
```

See [`test-stand/deployment/README.md`](test-stand/deployment/README.md) for the manifest layout and iteration tips.

### Test scenarios

| Scenario | What it exercises |
|---|---|
| `browse` | Catalog browse — shallow trace, single downstream hop |
| `inventory-http` | Inventory lookup over HTTP |
| `inventory-grpc` | Inventory lookup over gRPC |
| `checkout-http` | Full checkout flow over HTTP, no rollback |
| `checkout-grpc` | Full checkout flow over gRPC, no rollback |
| `checkout-rollback-grpc` | Checkout over gRPC with rollback — saga shape with three-way fan-out from checkout |
| `checkout-http-error` | Checkout over HTTP where payments returns 500 — exercises `error_count` on the HTTP path |
| `checkout-grpc-error` | Checkout over gRPC where inventory `ReserveItems` returns `codes.Internal` — exercises `error_count` on the gRPC path |
| `wide` | Gateway issues eight parallel downstream calls (catalog ×2, inventory-http ×2, inventory-grpc ×2, payments ×2 — distinct operation names) — pushes `breadth` to 8 |
| `deep` | `/deep` cycles through gateway → checkout → inventory-http → catalog → payments → gateway → … with a `depth` query parameter (default 8). Every hop is cross-service so `service_hop_depth` tracks `depth` linearly |
| `checkout-async-joined` | Checkout publishes to Kafka (trace context is always injected into headers); notifier consumes asynchronously, extracts the context, and joins the same trace — exercises messaging-derived operations on a non-root span and tests quiet-period accumulation across late-arriving spans. |
| `checkout-async-detached` | Same producer behavior as `joined` — headers carry trace context. The detached flag instructs the notifier consumer to **skip extraction**, so its span becomes a new root with a `messaging.operation.type=process` operation — exercises messaging-derived operations on a root span. |
| `catalog-db` | Gateway → catalog (HTTP) → Postgres `SELECT items` (CLIENT span with `db.system=postgresql`) — exercises operation derivation on non-HTTP/non-gRPC/non-messaging CLIENT spans (the `span.Name()` fallback). |
| `browse-cached` | Gateway hits Redis directly with `GET catalog:items` before falling through to catalog — same `db.system` fallback path on a different system; cache miss adds `redis.set` and an HTTP catalog hop, hit emits a single `redis.get` CLIENT span. |

Start with `browse` to confirm basic span ingestion, then use `checkout-rollback-grpc`, `checkout-grpc-error`, `wide`, `deep`, and `checkout-async-detached` to stress different topology dimensions.

### Continuous load

A `load-generator` Deployment lives in `test-stand/deployment/load-generator.yaml` but is **not** applied by `make demo-up`. Apply it separately when you want sustained traffic:

```sh
make demo-load-up      # apply the Deployment
make demo-load-logs    # tail it
make demo-load-down    # remove it
```

Configuration (env on the Deployment in `test-stand/deployment/load-generator.yaml`, edit and re-apply or `kubectl set env`):

| Variable | Default | Purpose |
|---|---|---|
| `RPS` | `10` | Target requests per second across the worker pool. The pool is fixed at 8 workers (capped at `RPS` for very low rates); the per-worker tick interval is derived from `RPS`. |
| `LOAD_PROFILE` | `demo` | Scenario mix preset. `demo` is a focused 8-scenario mix with ~2% errors — the controlled high-volume / rare-error path the before/after sampling demo arc is built around. `full` weights all fourteen scenarios (`wide`/`deep` doubled) for broad local-testing coverage. `custom` requires `MIX` to be set. |
| `MIX` | _(unset)_ | Comma-separated `name:weight` pairs. Honored **only** when `LOAD_PROFILE=custom`; ignored otherwise. |
| `ERROR_RATE_PCT` | _(unset)_ | Optional integer `0..100`. When set, error-scenario weights in the resolved mix are rescaled so the error rate hits the target; fails fast if the chosen mix has no errors. |
| `DEMO_LOG_VERBOSITY` | `modest` | Inherited from the shared `traced-service-env` ConfigMap. `heavy` enables `DEBUG` call sites and per-service heartbeats. |
| `START_DELAY` | `5s` | Delay before workers begin emitting (lets the stack stabilize). |
| `DURATION` | `0` | `0` = run until killed; any Go duration bounds the run for soak tests. |
| `GATEWAY_URL` | `http://gateway-api:8080` | Target gateway endpoint. |

### Messaging

The default stack includes a Kafka broker (`apache/kafka` in KRaft mode) and a `notifier` consumer service so the messaging trace shapes are always exercisable. Producer instrumentation in `checkout-api` activates only when a request carries `notify=joined|detached`; other scenarios are unaffected.

The producer and consumer wrappers in `internal/common/kafka.go` set `messaging.system=kafka`, `messaging.destination.name=<topic>`, and `messaging.operation.type=publish|process` on every span so tracealyzer's messaging operation derivation is exercised regardless of the upstream contrib's attribute conventions. The producer always injects trace context into Kafka headers via `ddtracer.Inject`. The detached vs joined distinction lives on the **consumer**: a `Detached` flag on `KafkaConsumerConfig` causes the consumer to skip `ddtracer.Extract`, so its span starts with no parent and becomes a new trace root.

### Database and cache

The default stack also includes a Postgres 16 instance (seeded from the `postgres-init` ConfigMap in `test-stand/deployment/postgres-init.yaml` with an `items` table) and a Redis 7 cache. The wrappers in `internal/common/db.go` and `internal/common/cache.go` annotate each query with `db.system`, `db.statement`, and `db.operation`, and emit `span.kind=client` with a generic span name (`postgres.query`, `redis.get`, `redis.set`). Operation derivation falls through to `span.Name()` for these spans, exercising the fallback path that no HTTP, gRPC, or messaging scenario reaches.

Catalog connects via `POSTGRES_DSN` and serves Postgres-backed responses on `?source=db`. Gateway connects via `REDIS_ADDR` and uses the cache on `?cache=true`. Both env vars are wired on the Deployments in `test-stand/deployment/demo-services.yaml`; Postgres and Redis expose readiness probes so the demo Pods come up after their dependencies are ready.

### Iterating on tracealyzer

```sh
# After a code change — rebuild, reload into kind, restart pod (skips helm).
make redeploy

# Full redeploy including chart changes.
make deploy-local
```

### Logs and metrics during a run

```sh
make logs                                              # tracealyzer log tail
make demo-collector-logs                               # OTel collector gateway logs
make demo-agent-logs                                   # Datadog agent (in-cluster DaemonSet) logs
curl -s localhost:9090/metrics | grep ^topology_      # requires make metrics-forward
```

The `topology_` metrics exposed are:

**Ingest and buffer:**

- `topology_spans_received_total` — spans accepted by the OTLP receivers.
- `topology_spans_malformed_total{stage="ingest"|"drain"}` — decoding failures, split by pipeline stage.
- `topology_buffer_rejected_total{reason="overflow"|"backend_error"}` — Valkey write rejections (maxmemory pressure vs. any other backend failure).

**Sweep and compute:**

- `topology_sweeps_total{result="ok"|"scan_error"|"emit_error"}` — sweep ticks, partitioned by outcome. `ok` includes ticks that found nothing finalizable.
- `topology_traces_finalized_total` — traces enqueued for emission, one increment per root. Success of the GreptimeDB write is tracked separately.
- `topology_finalization_trigger_total{trigger="quiet"|"max_ttl"}` — why each finalized trace was picked up.
- `topology_drain_errors_total` — per-trace Valkey `Drain` failures; the tick continues past them.
- `topology_compute_errors_total` — compute failures other than `ErrNoRoot`; non-zero indicates a bug for a specific trace shape.
- `topology_compute_skipped_total{reason="no_root"}` — traces skipped for a known reason during compute.
- `topology_orphan_spans_total` — spans unreachable from any root.
- `topology_orphan_bytes_total` — apportioned OTLP byte share of orphan spans; surfaces under-counts in per-root `span_bytes_total` at the org level.
- `topology_root_id_collisions_total` — rows merged away by the GreptimeDB source-table primary key `(root_id, trace_id)` because two or more authentic roots in one trace shared a `RootID`. The sweep WARN log carries `trace_id` and the merged-row count; the colliding `service::operation` is recoverable by querying the source table for that `trace_id`.
- `topology_compute_duration_seconds` (histogram) — per-trace compute latency.

**Emit:**

- `topology_emissions_failed_total` — topology rows dropped before a successful GreptimeDB write.

### Operational dashboard

A Grafana dashboard built on these metrics is shipped from the `mdai-hub` charts project (auto-loaded by the kube-prometheus-stack Grafana sidecar). It covers ingest/buffer rates, sweep and finalization triggers, compute latency percentiles, orphan spans/bytes, and emit success ratio, with `namespace` and `pod` template variables for multi-replica deployments.

### Tear down

```sh
make demo-down             # remove demo Deployments / Services / Jobs / ConfigMaps
make demo-delete-gateway   # remove the OTel collector gateway
```

## Local build

The repository includes the standard service scaffolding targets:

```sh
make tidy-check
make test
make build
```

The local build writes `./mdai-tracealyzer` from `cmd/mdai-tracealyzer/main.go`.

## Kubernetes deployment

Manifests for deploying the upstream collector live under `samples/`; the Helm chart for the tracealyzer service lives under `deployment/`.

The cluster contract once the service exists:

- Container ports: `4317` (OTLP gRPC), `4318` (OTLP HTTP), `9090` (Prometheus scrape + health probes).
- Readiness probe: `/healthz/ready` on `9090`; liveness probe: `/healthz/live` on `9090`. In v1 `/healthz/live` returns 200 unconditionally — deadlock detection is deferred.
- Configuration: ConfigMap for the YAML; Secrets for env-only values.
- `terminationGracePeriodSeconds` must be `≥ service.shutdown_grace` so the drain completes before SIGKILL.

## Configuration

Configuration is a YAML file overridable by environment variables:

- YAML path: `/etc/tracealyzer/config.yaml` by default, or `--config <path>`.
- Env-var naming: `<SECTION>_<FIELD>` (e.g. `BUFFER_VALKEY_ADDR`, `SERVICE_LOG_LEVEL`).
- Every YAML field can be overridden by its corresponding env var.

### CLI flags

- `--config <path>` — path to the YAML config file.
- `--migrate` — create or verify the GreptimeDB schema objects (`trace_root_topology`, `trace_root_topology_1m`, `trace_root_topology_1m_flow`) and exit. The Helm chart runs the binary with this flag from a `post-install,post-upgrade` Job (`deployment/templates/migration-job.yaml`, gated by `migrationJob.enabled`, default `true`) so the schema is in place before the Deployment becomes ready. Run it manually for out-of-cluster bootstraps.

### Env-only secrets

These are accepted **only** from environment variables; they are never read from the YAML file:

| Variable                              | Purpose                                    |
|---------------------------------------|--------------------------------------------|
| `BUFFER_VALKEY_PASSWORD`  | Valkey authentication.                     |
| `EMITTER_GREPTIMEDB_AUTH` | GreptimeDB ingester authentication token.  |

In Kubernetes deployments, inject both via a Secret. Do not commit them to `.env` files or the ConfigMap.

## Upstream collector requirement — Datadog 128-bit trace IDs

If the upstream OTel collector runs the `datadog` receiver (which converts Datadog Agent traffic into OTLP), **operators must enable the `receiver.datadogreceiver.Enable128BitTraceID` feature gate.** Without it, Datadog-origin traces arrive with the upper 64 bits of `trace_id` zeroed and the true upper half carried in a `_dd.p.tid` attribute — the "fragmentation mode" that splits logically-single traces across two trace IDs.

Tracealyzer treats the OTLP `trace_id` as authoritative and does not consult `_dd.p.tid`, so traces ingested in this fragmentation mode still emit metrics, but the dashboard `trace_id` will not match Datadog's native 128-bit ID. Enable the feature gate on the collector's `datadogreceiver` to keep trace IDs consistent end-to-end.

## License

AGPL-3.0 — see [`LICENSE`](LICENSE).
