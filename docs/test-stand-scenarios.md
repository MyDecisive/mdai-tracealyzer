# Test stand scenarios — manual verification

A reference for stepping through every scenario the demo exposes and confirming the rows tracealyzer emits to GreptimeDB match what the trace shape predicts.

## Before you start

1. Bring up the stack: `make demo-up`. Wait for `kafka`, `postgres`, and `redis` Deployments to report ready:
   ```sh
   kubectl get pods -l app.kubernetes.io/part-of=test-stand-demo
   ```
2. Make sure tracealyzer and the OTel collector gateway are already deployed in the same cluster (`make deploy-local` and `make demo-deploy-gateway`). Optional: tail tracealyzer logs (`make logs`) and metrics (`make metrics-forward`).
3. Tracealyzer's default `quiet_period` is **60 seconds**. After each emit, wait at least 65s before checking GreptimeDB. The `max_ttl` safety sweep is 10 minutes — if a scenario row still hasn't appeared after that, something's wrong upstream.
4. Decide how you'll read GreptimeDB. The HTTP API is the easiest:
   ```sh
   curl -s 'http://localhost:4000/v1/sql?db=public' \
     --data-urlencode 'sql=SELECT root_id, service_count, service_hop_depth, breadth, span_count, error_count FROM trace_root_topology ORDER BY timestamp DESC LIMIT 20'
   ```
   Adjust the host/port to match how you've port-forwarded GreptimeDB.

5. Recommended workflow per scenario:
   1. Note the current latest `timestamp` in `trace_root_topology`.
   2. Run the emit command.
   3. Wait 65 seconds.
   4. Query for rows newer than the noted timestamp and compare with the table below.

## Conventions used in the expected tables

- **`root_id`** — `service::operation`, derived per requirements §4.2.
- **`services`** — `service_count` column.
- **`hops`** — `service_hop_depth` column. Root counts as the first service.
- **`breadth`** — max direct children of any single span.
- **`spans`** — total `span_count` in the subtree.
- **`errors`** — spans with `Status.Code == ERROR`.

Span counts are *target* values. Real numbers can drift by ±1 if dd-trace-go decides to add or skip an internal span (e.g. the agent's transport span). Treat ±1 as acceptable; investigate larger gaps.

---

## 1. browse

```sh
make demo-emit DEMO_SCENARIO=browse
```

Trace shape:
```
gateway-api (server, GET /browse)
└── gateway-api (client, GET /catalog)
    └── catalog-api (server, GET /catalog)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /browse` | 2 | 2 | 1 | 3 | 0 |

---

## 2. inventory-http

```sh
make demo-emit DEMO_SCENARIO=inventory-http
```

Trace shape:
```
gateway-api (server, GET /inventory)
├── gateway-api (client, GET /catalog) → catalog-api (server)
└── gateway-api (client, GET /availability) → inventory-http-api (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /inventory` | 3 | 2 | 2 | 5 | 0 |

---

## 3. inventory-grpc

```sh
make demo-emit DEMO_SCENARIO=inventory-grpc
```

Trace shape:
```
gateway-api (server, GET /inventory)
├── gateway-api (client, GET /catalog) → catalog-api (server)
└── gateway-api (gRPC client, CheckAvailability) → inventory-grpc-service (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /inventory` | 3 | 2 | 2 | 5 | 0 |

Same numbers as scenario 2; the difference (and the point of running both) is the **transport mix** — verify in the DD UI or in the `operation` strings of internal spans that the second branch is `InventoryService/CheckAvailability` instead of `GET /availability`.

---

## 4. checkout-http

```sh
make demo-emit DEMO_SCENARIO=checkout-http
```

Trace shape:
```
gateway-api (server, GET /checkout)
└── gateway-api (client, GET /checkout) → checkout-api (server)
    ├── checkout-api (client, POST /reserve) → inventory-http-api (server)
    └── checkout-api (client, GET /authorize) → payments-api (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /checkout` | 4 | 3 | 2 | 7 | 0 |

---

## 5. checkout-grpc

```sh
make demo-emit DEMO_SCENARIO=checkout-grpc
```

Trace shape:
```
gateway-api (server, GET /checkout)
└── gateway-api (client, GET /checkout) → checkout-api (server)
    ├── checkout-api (gRPC client, ReserveItems) → inventory-grpc-service (server)
    └── checkout-api (client, GET /authorize) → payments-api (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /checkout` | 4 | 3 | 2 | 7 | 0 |

---

## 6. checkout-rollback-grpc

```sh
make demo-emit DEMO_SCENARIO=checkout-rollback-grpc
```

Payment is forced to decline; checkout calls `ReleaseReservation` to undo the reservation. No `STATUS_CODE_ERROR` spans (decline is a normal result), so `errors` stays at 0.

Trace shape:
```
gateway-api (server, GET /checkout)
└── gateway-api (client, GET /checkout) → checkout-api (server)
    ├── checkout-api (gRPC client, ReserveItems) → inventory-grpc-service (server)
    ├── checkout-api (client, GET /authorize) → payments-api (server)   [returns "declined"]
    └── checkout-api (gRPC client, ReleaseReservation) → inventory-grpc-service (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /checkout` | 4 | 3 | 3 | 9 | 0 |

`breadth` jumps to 3 (checkout fans out three downstream calls). This is the most topology-rich non-async scenario.

---

## 7. checkout-http-error

```sh
make demo-emit DEMO_SCENARIO=checkout-http-error
```

Payments returns HTTP 500. The error propagates up the call chain.

Trace shape (same span tree as `checkout-http`, with errors marked on the right branch):
```
gateway-api (server, GET /checkout) [error]
└── gateway-api (client, GET /checkout) [error] → checkout-api (server) [error]
    ├── checkout-api (client, POST /reserve) → inventory-http-api (server)
    └── checkout-api (client, GET /authorize) [error] → payments-api (server) [error]
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /checkout` | 4 | 3 | 2 | 7 | 5 |

`errors` = 5: three server spans (`gateway-api`, `checkout-api`, `payments-api` — all returning ≥ 500) plus two HTTP client spans whose responses were ≥ 500 (`gateway-api` client to `checkout-api` saw 502, `checkout-api` client to `payments-api` saw 500). Both server and client wrappers are configured with an explicit `status ≥ 500` predicate via `WithStatusCheck` / `RTWithStatusCheck`, so HTTP client errors are symmetric with server errors. The `checkout-api` client to `inventory-http-api` is not tagged because inventory returned 200. If you see 3, the `RTWithStatusCheck` on the client wrapper is missing or wrong; if you see 0, error tagging is being lost in the OTLP translation.

---

## 8. checkout-grpc-error

```sh
make demo-emit DEMO_SCENARIO=checkout-grpc-error
```

`ReserveItems` returns `codes.Internal`, so the trace dies before `payments-api` is reached.

Trace shape:
```
gateway-api (server, GET /checkout) [error]
└── gateway-api (client, GET /checkout) [error] → checkout-api (server) [error]
    └── checkout-api (gRPC client, ReserveItems) [error] → inventory-grpc-service (server) [error]
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /checkout` | 3 | 3 | 1 | 5 | 5 |

`services` is 3 because payments-api is never invoked. `breadth` is 1 (single chain).

`errors` = 5: `inventory-grpc-service` server (returned `codes.Internal`), `checkout-api` gRPC client (gRPC interceptor auto-tags non-OK status), `checkout-api` server (handler returned error → 500), `gateway-api` HTTP client to `checkout-api` (saw 500 — tagged by the explicit `RTWithStatusCheck(status ≥ 500)` policy), and `gateway-api` server.

---

## 9. wide

```sh
make demo-emit DEMO_SCENARIO=wide
```

Gateway issues eight parallel downstream calls — two per backing service, with distinct operation names so each appears as its own span.

Trace shape:
```
gateway-api (server, GET /wide)
├── gateway-api (client, GET /catalog)         → catalog-api (server)            [op: fetch_catalog]
├── gateway-api (client, GET /catalog)         → catalog-api (server)            [op: fetch_featured]
├── gateway-api (client, GET /availability)    → inventory-http-api (server)
├── gateway-api (client, POST /reserve)        → inventory-http-api (server)
├── gateway-api (gRPC client, CheckAvailability) → inventory-grpc-service (server)
├── gateway-api (gRPC client, ReserveItems)      → inventory-grpc-service (server)
├── gateway-api (client, GET /authorize)       → payments-api (server)           [op: preauthorize_payment]
└── gateway-api (client, GET /authorize)       → payments-api (server)           [op: quote_payment]
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /wide` | 5 | 2 | 8 | 17 | 0 |

`breadth` = 8 (gateway server has eight children). This is the highest-breadth scenario. `services` stays at 5 — each gateway-side call name disambiguates the span but the underlying services are still catalog-api, inventory-http-api, inventory-grpc-service, and payments-api alongside the gateway-api root.

---

## 10. deep

```sh
make demo-emit DEMO_SCENARIO=deep
```

Every role exposes a `/deep` endpoint that accepts a `depth` query parameter. Each invocation decrements `depth` and forwards to the **next service in a fixed cycle**, returning a leaf when `depth ≤ 1`. The cycle is:

```
gateway-api → checkout-api → inventory-http-api → catalog-api → payments-api → gateway-api → …
```

Every hop crosses a service boundary, so `service_hop_depth` grows linearly with `depth`. The load-generator scenario emits without a query string, so the gateway applies its default of 8.

Trace shape (default `depth=8` — eight service spans, wrapping around the cycle once):
```
gateway-api (server, GET /deep)                                            [depth=8]
└── gateway-api (client, GET /deep) → checkout-api (server)                [depth=7]
    └── checkout-api (client, GET /deep) → inventory-http-api (server)     [depth=6]
        └── inventory-http-api (client, GET /deep) → catalog-api (server)  [depth=5]
            └── catalog-api (client, GET /deep) → payments-api (server)    [depth=4]
                └── payments-api (client, GET /deep) → gateway-api (server)         [depth=3]
                    └── gateway-api (client, GET /deep) → checkout-api (server)     [depth=2]
                        └── checkout-api (client, GET /deep) → inventory-http-api (server) [depth=1, leaf]
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /deep` | 5 | 8 | 1 | 15 | 0 |

`spans` = 15: eight server spans (one per role visit) plus seven client spans (one between each pair). `breadth` = 1 — every span has at most one child.

`hops` = 8 because every parent→child crosses a service boundary, and `service_hop_depth` increments on each cross-service transition (revisits count). `services` = 5 distinct: `gateway-api`, `checkout-api`, `inventory-http-api`, `catalog-api`, `payments-api`. Use this scenario to verify that hop depth tracks span chain length when the chain is genuinely cross-service.

To make `hops` lower than 8 without rebuilding, supply a smaller `depth`:

```sh
curl -s 'http://localhost:8081/deep?scenario=deep&depth=4'
```

`depth=4` produces `gateway → checkout → inventory-http → catalog`, giving `hops=4`, `services=4`, `spans=7`.

---

## 11. checkout-async-joined

```sh
make demo-emit DEMO_SCENARIO=checkout-async-joined
```

Checkout publishes to Kafka — the producer always injects DD/W3C trace context into Kafka headers via `ddtracer.Inject`. Notifier consumes asynchronously, runs `ddtracer.Extract` on the headers, and starts its consumer span as a child of the producer's span context. Both ends of the chain end up on the same trace.

Trace shape (single trace):
```
gateway-api (server, GET /checkout)
└── gateway-api (client, GET /checkout) → checkout-api (server)
    ├── checkout-api (gRPC client, ReserveItems) → inventory-grpc-service (server)
    ├── checkout-api (client, GET /authorize) → payments-api (server)
    └── checkout-api (kafka producer, publish checkout-completed-joined)
        └── notifier (kafka consumer, process checkout-completed-joined)   [joined]
            └── notifier (client, GET /catalog) → catalog-api (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /checkout` | 6 | 4 | 3 | 11 | 0 |

`services` jumps to 6 because the consumer adds `notifier` and `catalog-api` to the subtree. `hops` = 4 along the path gateway → checkout → notifier → catalog. `breadth` = 3 from checkout's three children.

If you see two rows instead of one, trace-context propagation through the Kafka headers is broken (consumer became a separate root).

---

## 12. checkout-async-detached

```sh
make demo-emit DEMO_SCENARIO=checkout-async-detached
```

Producer behavior is identical to `joined` — it injects trace context into Kafka headers either way. The difference is on the consumer: the `Detached: true` flag on `KafkaConsumerConfig` causes the consumer to **skip `ddtracer.Extract`**, so its span starts with no parent context and becomes a new trace root. The Kafka headers still carry the original trace IDs, but the consumer ignores them.

Trace A — synchronous request:
```
gateway-api (server, GET /checkout)
└── gateway-api (client, GET /checkout) → checkout-api (server)
    ├── checkout-api (gRPC client, ReserveItems) → inventory-grpc-service (server)
    ├── checkout-api (client, GET /authorize) → payments-api (server)
    └── checkout-api (kafka producer, publish checkout-completed-detached)
```

Trace B — async detached consumer:
```
notifier (kafka consumer, process checkout-completed-detached)
└── notifier (client, GET /catalog) → catalog-api (server)
```

Expected rows: **2**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /checkout` | 4 | 3 | 3 | 8 | 0 |
| `notifier::process checkout-completed-detached` | 2 | 2 | 1 | 3 | 0 |

The second row is the only place the §4.2 messaging branch fires for a **root** span. If `root_operation` reads `kafka.process` instead of `process checkout-completed-detached`, the messaging attributes (`messaging.operation.type`, `messaging.destination.name`) aren't reaching tracealyzer through the OTLP translation — the operation is falling through to `span.Name()`.

The two rows arrive ~1 second apart. Both finalize after their own quiet period.

---

## 13. catalog-db

```sh
make demo-emit DEMO_SCENARIO=catalog-db
```

Trace shape:
```
gateway-api (server, GET /browse)
└── gateway-api (client, GET /catalog) → catalog-api (server)
    └── catalog-api (postgres CLIENT, postgres.query SELECT items)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /browse` | 2 | 2 | 1 | 4 | 0 |

The DB span has `service.name=catalog-api` (it's a client span on catalog), so `services` stays at 2. The point of this scenario is to confirm the §4.2 fallback path: the postgres span carries no HTTP/gRPC/messaging attributes, so its `operation` should derive from `span.Name()` = `postgres.query`. Verify by inspecting any internal span in the trace, not the root.

---

## 14. browse-cached

```sh
make demo-emit DEMO_SCENARIO=browse-cached
```

Trace shape varies between cold and warm cache.

**Cold** (cache empty — first emit after stack startup or after `redis-cli FLUSHALL`):
```
gateway-api (server, GET /browse)
├── gateway-api (redis CLIENT, redis.get catalog:items)   [miss]
├── gateway-api (client, GET /catalog) → catalog-api (server)
└── gateway-api (redis CLIENT, redis.set catalog:items)
```

**Warm** (every subsequent emit):
```
gateway-api (server, GET /browse)
└── gateway-api (redis CLIENT, redis.get catalog:items)   [hit]
```

Expected rows: **1** in either case.

| state | root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|---|
| cold | `gateway-api::GET /browse` | 2 | 2 | 3 | 5 | 0 |
| warm | `gateway-api::GET /browse` | 1 | 1 | 1 | 2 | 0 |

The Redis spans use `service.name=gateway-api`, which is why `services` drops to 1 in the warm case. Same `services`-doesn't-grow logic as `catalog-db`: a CLIENT span on the same service doesn't introduce a new service hop.

To force a cold call again:
```sh
kubectl exec deployment/redis -- redis-cli FLUSHALL
```

---

## When numbers don't match

Quick triage table:

| Symptom | Likely cause |
|---|---|
| Zero rows for a scenario, even after 65s | trace not finalizing — check `topology_traces_finalized_total` and `topology_orphan_spans_total`; if orphans rose, span propagation is broken upstream |
| `services` is one higher than expected on every scenario | demo-control-style intermediate hop slipped back in, or load-generator is being counted as a service |
| `services` is one lower than expected | a downstream service is dying silently — check `kubectl get pods -l app.kubernetes.io/part-of=test-stand-demo` |
| Multiple rows for a single-trace scenario (1 → 2+) | trace-context propagation breaking at one of the service boundaries; check `dd.trace_id` consistency in service logs |
| Detached scenario only produces one row | Kafka producer or consumer not running; see the notifier troubleshooting in `README.md` |
| `errors` always 0 even on error scenarios | error tagging is being dropped by datadogreceiver; check the OTel collector logs for the translation |
| `root_operation` is `kafka.process` instead of `process <topic>` | messaging attributes not reaching tracealyzer — check what attribute keys actually arrive in OTLP |

For a clean baseline between runs, you can reset the topology table:

```sql
DELETE FROM trace_root_topology;
```

Or filter by recent timestamp in your verification queries.
