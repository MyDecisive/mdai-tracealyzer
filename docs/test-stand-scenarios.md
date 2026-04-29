# Test stand scenarios — manual verification

A reference for stepping through every scenario the demo exposes and confirming the rows tracealyzer emits to GreptimeDB match what the trace shape predicts.

## Before you start

1. Bring up the stack: `make demo-up`. Wait for `kafka`, `postgres`, and `redis` to report healthy:
   ```sh
   docker compose -f test-stand/docker-compose.yaml ps
   ```
2. Make sure tracealyzer is deployed and the gateway port-forward is up (`make demo-port-forward`). Optional: tail tracealyzer logs (`make logs`) and metrics (`make metrics-forward`).
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
| `gateway-api::GET /checkout` | 4 | 3 | 2 | 7 | 3 |

`errors` = 3 because only the server spans are tagged: `gateway-api` server, `checkout-api` server, and `payments-api` server. dd-trace-go's HTTP **client** integration does not auto-tag 5xx responses as errors — its convention is that the server records the failure and the client only records transport-level failures (network, timeout, connect refused). If you see a 0, error tagging is being lost in the OTLP translation; if you see >3, somebody added client-side error tagging via `httptrace.RTWithErrorCheck` or similar.

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
| `gateway-api::GET /checkout` | 3 | 3 | 1 | 5 | 4 |

`services` is 3 because payments-api is never invoked. `breadth` is 1 (single chain).

`errors` = 4: `inventory-grpc-service` server (returned `codes.Internal`), `checkout-api` gRPC client (the gRPC client interceptor *does* auto-tag non-OK status, unlike the HTTP client), `checkout-api` server (handler returned error → 500), and `gateway-api` server. The `gateway-api` HTTP client to `checkout-api` is *not* tagged for the same reason as scenario 7.

---

## 9. wide

```sh
make demo-emit DEMO_SCENARIO=wide
```

Gateway calls four downstream services concurrently.

Trace shape:
```
gateway-api (server, GET /wide)
├── gateway-api (client, GET /catalog) → catalog-api (server)
├── gateway-api (client, GET /availability) → inventory-http-api (server)
├── gateway-api (gRPC client, CheckAvailability) → inventory-grpc-service (server)
└── gateway-api (client, GET /authorize) → payments-api (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /wide` | 5 | 2 | 4 | 9 | 0 |

`breadth` = 4 (gateway server has four children). This is the highest-breadth scenario.

---

## 10. deep

```sh
make demo-emit DEMO_SCENARIO=deep
```

Trace shape:
```
gateway-api (server, GET /deep)
└── gateway-api (client, GET /deep) → checkout-api (server, GET /deep)
    └── checkout-api (client, GET /deep-check) → inventory-http-api (server, GET /deep-check)
        └── inventory-http-api (client, GET /catalog) → catalog-api (server)
```

Expected rows: **1**.

| root_id | services | hops | breadth | spans | errors |
|---|---|---|---|---|---|
| `gateway-api::GET /deep` | 4 | 4 | 1 | 7 | 0 |

`hops` = 4 is the highest-depth scenario. Use this to verify the hop-depth metric responds to chain length.

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
docker compose -f test-stand/docker-compose.yaml exec redis redis-cli FLUSHALL
```

---

## When numbers don't match

Quick triage table:

| Symptom | Likely cause |
|---|---|
| Zero rows for a scenario, even after 65s | trace not finalizing — check `topology_traces_finalized_total` and `topology_orphan_spans_total`; if orphans rose, span propagation is broken upstream |
| `services` is one higher than expected on every scenario | demo-control-style intermediate hop slipped back in, or load-generator is being counted as a service |
| `services` is one lower than expected | a downstream service is dying silently — check `docker compose ps` |
| Multiple rows for a single-trace scenario (1 → 2+) | trace-context propagation breaking at one of the service boundaries; check `dd.trace_id` consistency in service logs |
| Detached scenario only produces one row | Kafka producer or consumer not running; see the notifier troubleshooting in `README.md` |
| `errors` always 0 even on error scenarios | error tagging is being dropped by datadogreceiver; check the OTel collector logs for the translation |
| `root_operation` is `kafka.process` instead of `process <topic>` | messaging attributes not reaching tracealyzer — check what attribute keys actually arrive in OTLP |

For a clean baseline between runs, you can reset the topology table:

```sql
DELETE FROM trace_root_topology;
```

Or filter by recent timestamp in your verification queries.
