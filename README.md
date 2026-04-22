# mdai-tracealyzer

Trace topology service — computes structural metrics from OTLP traces and writes them to GreptimeDB for tail-sampling decisions.

## What it does

The v1 target: ingest OTLP spans (gRPC on `4317`, HTTP on `4318`), buffer them per trace in Valkey, and — once a trace is quiet for long enough or hits its max TTL — compute topology metrics (breadth, service-hop depth, service/operation/span/error counts, root duration) and write one row per root to the `trace_root_topology` table in GreptimeDB. A dashboard built on that table surfaces traces worth keeping for tail-sampling decisions.

**Current build status:** OTLP ingest and the Valkey buffer are implemented. Sweep (quiet/max_ttl finalization), topology compute, and the GreptimeDB emitter are not yet wired up — spans are accepted and buffered, but no rows are written to GreptimeDB in this build.

The service does not make the sampling decision itself, store full trace data, render the dashboard, or alert. Span-link handling and genuinely multi-root traces are deferred past v1.

## Running the test-stand demo

The `test-stand/` tree exercises the upstream side — a Datadog agent plus a handful of demo microservices that emit realistic traces. The flow is: demo services → Datadog agent → OTel collector gateway (kind) → tracealyzer (kind).

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

# Emit a scenario.
make demo-emit DEMO_SCENARIO=browse
```

### Test scenarios

| Scenario | What it exercises |
|---|---|
| `browse` | Catalog browse — shallow trace, single service |
| `inventory-http` | Inventory lookup over HTTP |
| `inventory-grpc` | Inventory lookup over gRPC |
| `checkout-http` | Full checkout flow over HTTP, no rollback |
| `checkout-grpc` | Full checkout flow over gRPC, no rollback |
| `checkout-rollback-grpc` | Checkout over gRPC with rollback — error path, deepest service graph |

Start with `browse` to confirm basic span ingestion, then use `checkout-rollback-grpc` to stress topology computation with error spans.

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

The ingest/buffer counters currently exposed are:

- `topology_spans_received_total` — spans accepted by the OTLP receivers.
- `topology_spans_malformed_total{stage="ingest"|"drain"}` — decoding failures, split by pipeline stage.
- `topology_buffer_rejected_total{reason="overflow"|"backend_error"}` — Valkey write rejections (maxmemory pressure vs. any other backend failure).

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
- Env-var naming: `TRACEALYZER_<SECTION>_<FIELD>` (e.g. `TRACEALYZER_BUFFER_VALKEY_ADDR`, `TRACEALYZER_SERVICE_LOG_LEVEL`).
- Every YAML field can be overridden by its corresponding env var.

### Env-only secrets

These are accepted **only** from environment variables; they are never read from the YAML file:

| Variable                              | Purpose                                    |
|---------------------------------------|--------------------------------------------|
| `TRACEALYZER_BUFFER_VALKEY_PASSWORD`  | Valkey authentication.                     |
| `TRACEALYZER_EMITTER_GREPTIMEDB_AUTH` | GreptimeDB ingester authentication token.  |

In Kubernetes deployments, inject both via a Secret. Do not commit them to `.env` files or the ConfigMap.

## Upstream collector requirement — Datadog 128-bit trace IDs

If the upstream OTel collector runs the `datadog` receiver (which converts Datadog Agent traffic into OTLP), **operators must enable the `receiver.datadogreceiver.Enable128BitTraceID` feature gate.** Without it, Datadog-origin traces arrive with the upper 64 bits of `trace_id` zeroed and the true upper half carried in a `_dd.p.tid` attribute — the "fragmentation mode" that splits logically-single traces across two trace IDs.

Tracealyzer treats the OTLP `trace_id` as authoritative and does not consult `_dd.p.tid`, so traces ingested in this fragmentation mode still emit metrics, but the dashboard `trace_id` will not match Datadog's native 128-bit ID. Enable the feature gate on the collector's `datadogreceiver` to keep trace IDs consistent end-to-end.

## License

AGPL-3.0 — see [`LICENSE`](LICENSE).
