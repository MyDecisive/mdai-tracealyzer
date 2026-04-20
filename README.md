# mdai-tracealyzer

Trace topology service — computes structural metrics from OTLP traces and writes them to GreptimeDB for tail-sampling decisions.

## What it does

The service ingests OTLP spans (gRPC on `4317`, HTTP on `4318`), buffers them per trace in Valkey, and — once a trace is quiet for long enough or hits its max TTL — computes topology metrics (breadth, service-hop depth, service/operation/span/error counts, root duration) and writes one row per root to the `trace_topology` table in GreptimeDB. A dashboard built on that table surfaces traces worth keeping for tail-sampling decisions.

The service does not make the sampling decision itself, store full trace data, render the dashboard, or alert. Span-link handling and genuinely multi-root traces are deferred past v1.

## Running the test-stand demo

The `test-stand/` tree exercises the upstream side — a Datadog agent plus a handful of demo microservices that emit realistic traces. The current `mdai-tracealyzer` binary is a packaging scaffold with health endpoints; OTLP ingestion, buffering, topology computation, and GreptimeDB emission are still represented by the requirements and design documents.

Today's flow exercises the Datadog receiver → OTel collector gateway path, which is the upstream contract tracealyzer will consume:

```sh
# 1. Deploy the test gateway (OpenTelemetry Collector with the Datadog receiver)
#    to the local kind cluster.
make demo-deploy-gateway

# 2. Port-forward the gateway so the local demo services can reach it.
make demo-port-forward

# 3. Bring up the demo services (Datadog agent + catalog/checkout/inventory/...)
#    under docker compose.
make demo-up

# 4. Emit a scenario through the demo control API.
make demo-emit DEMO_SCENARIO=browse

# Logs:
make demo-gateway-logs    # collector-side
make demo-agent-logs      # Datadog agent-side
```

Tear down with `make demo-down` and `make demo-delete-gateway`.

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
