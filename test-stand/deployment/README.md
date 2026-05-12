# Test-stand demo — Kubernetes manifests

Raw manifests that run the full demo stack (catalog, checkout, gateway, payments, inventory-http, inventory-grpc, notifier, Postgres, Redis, Kafka, Datadog agent, load-generator) in the same cluster as tracealyzer. Traces flow end-to-end without any port-forward bridging.

Layout:

- `postgres-init.yaml` — ConfigMap with the `items` table seed SQL.
- `stateful.yaml` — Postgres, Redis, Kafka (single-replica each) plus a Job that creates the two `checkout-completed-*` topics.
- `datadog-agent.yaml` — DaemonSet running the official Datadog agent in APM + logs mode, plus its ServiceAccount and a `datadog-agent-$(DEMO_NAMESPACE)` ClusterRole/ClusterRoleBinding granting read access to `nodes`, `namespaces`, `pods`, `endpoints`, and `endpointslices` (needed for kubelet metadata + log enrichment). `DD_CONTAINER_INCLUDE=kube_namespace:^$(DEMO_NAMESPACE)$` scopes autodiscovery to the demo namespace, so the agent never opens log files for pods elsewhere. Forwards APM (`DD_APM_DD_URL`) and logs (`DD_LOGS_CONFIG_LOGS_DD_URL`) to `DEMO_DATADOG_FORWARDER_URL`, substituted at apply time by the Makefile. Mounts `/var/log/pods` and `/var/log/containers` from the host, plus `/opt/datadog-agent/run` for pointer-file state.
- `demo-services.yaml` — A shared `traced-service-env` ConfigMap (DD_* env, `DEMO_LOG_VERBOSITY`) plus the seven role Deployments and their Services. Every Deployment uses the same `test-stand-demo-svc:local` image, differentiated by `DEMO_ROLE` and per-role env wiring.
- `load-generator.yaml` — Standalone Deployment for the `test-stand-load-generator:local` image. `LOAD_PROFILE`, `RPS`, `ERROR_RATE_PCT`, `MIX` exposed inline; edit and re-apply, or override via `kubectl set env`.

The demo stack lives in the `test-stand-demo` namespace (created by `make demo-up`, override with `DEMO_NAMESPACE=…`), separate from the namespace where tracealyzer and the OTel collector gateway run (`NAMESPACE`, default `mdai`). The in-cluster Datadog agent reaches the gateway across namespaces via the configured fully-qualified Service DNS name.

## Prerequisites

The Datadog agent reads `DD_API_KEY` from a Secret with key `api-key`. The source Secret lives in `$(NAMESPACE)` (where tracealyzer runs); `make demo-apply` copies it into `$(DEMO_NAMESPACE)` automatically before applying the DaemonSet.

Create the source Secret once:

```sh
kubectl --context=kind-$KIND_CLUSTER_NAME -n mdai \
    create secret generic test-vv-integration-secret --from-literal=api-key=<DD_API_KEY>
```

Override the name via `DEMO_DATADOG_SECRET_NAME` if your Secret follows a different convention:

```sh
make demo-apply DEMO_DATADOG_SECRET_NAME=prod-dd-integration-secret
```

`demo-apply` errors out with the create command if the source Secret is missing.

## Forwarder modes

Two ways to wire the Datadog agent at the cluster-side trace ingest:

**A. In-project sample collector** (default). Deploy the sample `OpenTelemetryCollector` defined under `samples/` (kustomize base) and the demo agent forwards to it:

```sh
make demo-deploy-gateway          # kubectl apply -k samples/
make demo-apply                   # forwards to datadog-agent-test-gateway-collector.$(NAMESPACE)…
```

To run a renamed instance, add a kustomize overlay under `samples/overlays/<name>/` that patches `metadata.name`, then apply it with `kubectl apply -k samples/overlays/<name>/`. The default forwarder URL is fixed to the base name; if you rename, override `DEMO_DATADOG_FORWARDER_URL` to match.

**B. Existing external endpoint** (e.g. an `mdai-envoy` Service already running in the cluster). Skip `demo-deploy-gateway` and provide the full URL:

```sh
make demo-apply-external DEMO_DATADOG_FORWARDER_URL=http://mdai-envoy.mdai.svc.cluster.local:8126
```

`demo-apply-external` requires `DEMO_DATADOG_FORWARDER_URL` to be set explicitly so the Mode A default is never silently used here.

## Bring it up

```sh
# Build both demo images, load them into kind, apply the core stack.
make demo-up

# (Optional) start continuous load — same image, separate Deployment.
make demo-load-up
```

## Emit a single scenario

The load-generator binary has a `--once <scenario>` mode. The Makefile target runs it as a one-shot Pod with `kubectl run --rm --attach`, so the output streams back to your shell and the Pod is cleaned up on exit:

```sh
make demo-emit DEMO_SCENARIO=browse
```

For free-form curls or a sustained shell loop against the gateway, port-forward instead:

```sh
make demo-app-port-forward         # service/gateway-api 8081:8080
curl -s 'http://localhost:8081/wide?scenario=wide'
```

## Tear it down

```sh
make demo-load-down                # stop the load-generator only
make demo-down                     # remove all demo resources, including the
                                   # cluster-scoped datadog-agent-$(DEMO_NAMESPACE)
                                   # ClusterRole/ClusterRoleBinding
```

The OTel collector gateway and tracealyzer are unaffected — they're managed separately via `make demo-deploy-gateway` and `make deploy-local`.

## Iterating on code

After editing `cmd/demo-svc` or `cmd/load-generator`, rebuild only the image you changed and trigger a rolling restart:

```sh
make demo-svc-kind-load && make demo-rollout            # demo-svc changes
make demo-loadgen-kind-load && make demo-load-rollout   # load-generator changes
```

Manifest-only changes don't need rebuilding:

```sh
make demo-apply                                         # re-apply core manifests (no docker build, no kind-load)
```

Or for a clean re-roll:

```sh
make demo-down
make demo-up
```
