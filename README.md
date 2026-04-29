# mdai-tracealyzer

Trace topology service — computes structural metrics from OTLP traces and writes them to GreptimeDB for tail-sampling decisions.

## What it does

The v1 target: ingest OTLP spans (gRPC on `4317`, HTTP on `4318`), buffer them per trace in Valkey, and — once a trace is quiet for long enough or hits its max TTL — compute topology metrics (breadth, service-hop depth, service/operation/span/error counts, root duration) and write one row per root to the `trace_root_topology` table in GreptimeDB. A dashboard built on that table surfaces traces worth keeping for tail-sampling decisions.

The service does not make the sampling decision itself, store full trace data, render the dashboard, or alert. Span-link handling and genuinely multi-root traces are deferred past v1.

## Running the test-stand demo

The `test-stand/` tree exercises the upstream side — a Datadog agent plus a handful of demo microservices that emit realistic traces. The flow is: demo services → Datadog agent → OTel collector gateway (kind) → tracealyzer (kind).

Every demo container (catalog, checkout, gateway, payments, inventory-http, inventory-grpc, notifier) is one role of a single `demo-svc` binary; compose runs it seven times with distinct `DEMO_ROLE` and `DD_SERVICE` env vars. Adding a new role means a new `cmd/demo-svc/role_<name>.go` plus a compose entry — no new module to wire.

### One-time setup

```sh
# Deploy the OTel collector gateway to the kind cluster.
make demo-deploy-gateway

# Build and deploy tracealyzer to the kind cluster.
make deploy-local
```

### Per-session

Open three terminals:

```sh
# Terminal 1 — port-forward the gateway so docker-compose services can reach it.
make demo-port-forward

# Terminal 2 — port-forward tracealyzer metrics (optional, for curl localhost:9090/metrics).
make metrics-forward

# Terminal 3 — tail tracealyzer logs.
make logs
```

Then in a fourth terminal:

```sh
# Bring up the demo services (Datadog agent + catalog/checkout/inventory/...).
make demo-up

# Emit a scenario — curls gateway-api directly via the host port published by compose.
make demo-emit DEMO_SCENARIO=browse
```

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
| `wide` | Gateway fans out concurrently to catalog, inventory-http, inventory-grpc, and payments — pushes `breadth` past 3 |
| `deep` | gateway → checkout → inventory-http → catalog — pushes `service_hop_depth` to 4 |
| `checkout-async-joined` | Checkout publishes to Kafka (trace context is always injected into headers); notifier consumes asynchronously, extracts the context, and joins the same trace — exercises messaging-derived operations on a non-root span and tests quiet-period accumulation across late-arriving spans. |
| `checkout-async-detached` | Same producer behavior as `joined` — headers carry trace context. The detached flag instructs the notifier consumer to **skip extraction**, so its span becomes a new root with a `messaging.operation.type=process` operation — exercises messaging-derived operations on a root span. |
| `catalog-db` | Gateway → catalog (HTTP) → Postgres `SELECT items` (CLIENT span with `db.system=postgresql`) — exercises operation derivation on non-HTTP/non-gRPC/non-messaging CLIENT spans (the §4.2 fallback path). |
| `browse-cached` | Gateway hits Redis directly with `GET catalog:items` before falling through to catalog — same `db.system` fallback path on a different system; cache miss adds `redis.set` and an HTTP catalog hop, hit emits a single `redis.get` CLIENT span. |

Start with `browse` to confirm basic span ingestion, then use `checkout-rollback-grpc`, `checkout-grpc-error`, `wide`, `deep`, and `checkout-async-detached` to stress different topology dimensions.

### Continuous load

A `load-generator` service is included in the compose stack behind a `load` profile so it stays off by default. It hits `gateway-api` on a configurable interval with a weighted-random scenario mix.

```sh
make demo-load-up      # start
make demo-load-logs    # tail
make demo-load-down    # stop and remove
```

Tunables (env on the service in `test-stand/docker-compose.yaml`): `INTERVAL` (Go duration, default `1s`), `CONCURRENCY` (default `1`), `MIX` (`name:weight` pairs, comma-separated), `DURATION` (`0` = forever, otherwise a Go duration for bounded soak runs), `START_DELAY`, `GATEWAY_URL`. The default mix covers all fourteen scenarios with `wide` and `deep` doubled.

### Messaging

The default stack includes a Kafka broker (`apache/kafka` in KRaft mode) and a `notifier` consumer service so the messaging trace shapes are always exercisable. Producer instrumentation in `checkout-api` activates only when a request carries `notify=joined|detached`; other scenarios are unaffected.

The producer and consumer wrappers in `internal/common/kafka.go` set `messaging.system=kafka`, `messaging.destination.name=<topic>`, and `messaging.operation.type=publish|process` on every span so tracealyzer's messaging operation derivation is exercised regardless of the upstream contrib's attribute conventions. The producer always injects trace context into Kafka headers via `ddtracer.Inject`. The detached vs joined distinction lives on the **consumer**: a `Detached` flag on `KafkaConsumerConfig` causes the consumer to skip `ddtracer.Extract`, so its span starts with no parent and becomes a new trace root.

### Database and cache

The default stack also includes a Postgres 16 instance (seeded from `test-stand/postgres/init.sql` with an `items` table) and a Redis 7 cache. The wrappers in `internal/common/db.go` and `internal/common/cache.go` annotate each query with `db.system`, `db.statement`, and `db.operation`, and emit `span.kind=client` with a generic span name (`postgres.query`, `redis.get`, `redis.set`). Operation derivation falls through to `span.Name()` for these spans, exercising the §4.2 fallback that no HTTP, gRPC, or messaging scenario reaches.

Catalog connects via `POSTGRES_DSN` and serves Postgres-backed responses on `?source=db`. Gateway connects via `REDIS_ADDR` and uses the cache on `?cache=true`. Both env vars are wired in compose; service startup is gated on the Postgres and Redis healthchecks so the demo apps don't crash-loop while the dependencies come up.

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
make demo-gateway-logs                                 # OTel collector gateway logs
make demo-agent-logs                                   # Datadog agent logs
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
- `topology_compute_duration_seconds` (histogram) — per-trace compute latency.

**Emit:**

- `topology_emissions_failed_total` — topology rows dropped before a successful GreptimeDB write.

### Tear down

```sh
make demo-down             # stop docker-compose services
make demo-delete-gateway   # remove gateway from kind
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
