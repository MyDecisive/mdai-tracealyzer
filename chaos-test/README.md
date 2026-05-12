# Tracealyzer Chaos Testing Proposal

This document describes a proposed chaos testing framework for Tracealyzer. The
goal is not to validate only the current implementation details, but to define a
repeatable way to test Tracealyzer behavior when its dependencies, network, and
runtime environment fail.

Tracealyzer's dependency path is:

```text
OTLP load -> Tracealyzer ingest -> Valkey buffer -> Tracealyzer sweeper -> GreptimeDB
```

The chaos suite should answer these questions:

- Does Tracealyzer keep accepting, rejecting, or back-pressuring payloads in a predictable way?
- Are accepted traces eventually represented in GreptimeDB when dependencies recover?
- Are expected loss modes visible through metrics and logs?
- Does readiness reflect downstream availability well enough to protect operators?
- Does the service recover without manual cleanup after dependency and pod failures?

## Chaos Scenarios

### GreptimeDB Write Path Unavailable

GreptimeDB ingester is unreachable while Tracealyzer has finalizable traces.

Expected validation:

- Tracealyzer does not crash.
- Write failures are visible through metrics.
- If the outage is shorter than the configured retry window, rows should arrive in GreptimeDB.
- If the outage is longer than the configured retry window, the framework should detect and report whether rows were lost.

Fault types:

- Network partition between Tracealyzer and GreptimeDB ingester.
- Packet loss or timeout on GreptimeDB ingester traffic.
- GreptimeDB pod/container kill.
- GreptimeDB service DNS failure.

### GreptimeDB SQL/Readiness Path Unavailable

GreptimeDB SQL endpoint is unavailable while the service starts or runs.

Expected validation:

- Tracealyzer readiness should remain false during startup if schema readiness cannot be verified.
- A running service should expose observable readiness or health behavior when the SQL endpoint becomes unavailable.
- The sweeper should not finalize traces before required downstream readiness gates are satisfied.

Fault types:

- Network partition to GreptimeDB SQL endpoint.
- GreptimeDB SQL pod/container kill.
- DNS failure for GreptimeDB service name.

### Valkey Unavailable During Startup

Tracealyzer starts while Valkey is unreachable.

Expected validation:

- Tracealyzer should not report ready until it can initialize its buffer dependency.
- Startup retry behavior should be observable.
- Recovery should happen when Valkey becomes reachable.

Fault types:

- Valkey pod/container not running.
- Network partition from Tracealyzer to Valkey.
- DNS failure for Valkey service name.

### Valkey Unavailable During Ingest

Tracealyzer receives OTLP payloads while Valkey cannot accept writes.

Expected validation:

- Tracealyzer returns a deterministic OTLP partial success or rejection behavior.
- Rejected span counts are visible in metrics.
- The load generator can measure how many spans were sent, accepted, rejected, and later materialized in GreptimeDB.

Fault types:

- Network partition between Tracealyzer and Valkey.
- Valkey pod/container kill.
- Valkey response delay or packet loss.

### Valkey Unavailable During Sweep

Valkey becomes unavailable while Tracealyzer scans or drains finalizable traces.

Expected validation:

- Scan and drain errors are visible through metrics.
- Tracealyzer continues running.
- Traces that were not successfully drained should be retried after Valkey recovers.
- Duplicate or missing GreptimeDB rows should be detected.

Fault types:

- Network partition during sweep interval.
- Valkey pod/container kill.
- Valkey latency injection.

### Valkey Memory Pressure

Valkey is configured with low memory and a policy that rejects writes.

Expected validation:

- Tracealyzer classifies buffer overflow separately from generic backend errors.
- Rejected spans are visible through metrics.
- The load generator can correlate sent spans with rejected spans and missing topology rows.

Fault types:

- Low `maxmemory`.
- `noeviction` policy.
- High ingest rate.

### Tracealyzer Pod Kill

Tracealyzer is killed during different pipeline phases.

Expected validation:

- Kubernetes restarts the pod.
- Readiness eventually recovers.
- The framework can identify whether data accepted before the kill was eventually emitted.
- GreptimeDB rows should not be duplicated unexpectedly after restart.

Fault timing variants:

- During ingest.
- During quiet period before sweep.
- During sweep/drain.
- During emit retries.

### Tracealyzer Graceful Shutdown

Tracealyzer receives a normal termination signal while ingest, sweep, or emit is active.

Expected validation:

- Shutdown completes within the configured grace period.
- Pending rows are flushed when dependencies are available.
- If dependencies are unavailable, failure is visible and deterministic.

Fault types:

- Kubernetes pod deletion.
- Rolling restart.
- Dependency outage during shutdown.

### Slow Dependency Responses

Valkey or GreptimeDB responds slowly but does not fully fail.

Expected validation:

- Timeouts, retries, and backoff behave as configured.
- Ingest latency and sweep duration remain bounded or are observable.
- Queue growth and dropped rows are visible.

Fault types:

- Network latency.
- Packet loss.
- Bandwidth limit.

### DNS Failure

Tracealyzer cannot resolve Valkey or GreptimeDB service names.

Expected validation:

- Startup and runtime behavior is deterministic.
- Readiness and metrics make the issue diagnosable.
- Recovery occurs after DNS is restored.

Fault types:

- Kubernetes DNS chaos.
- Local resolver/proxy failure in Docker Compose.

### High Load With Dependency Degradation

Tracealyzer receives sustained load while one dependency is degraded.

Expected validation:

- Queue limits and buffer rejection behavior are measurable.
- Metrics remain available.
- Tracealyzer avoids unbounded memory growth.
- Recovery after the fault does not require manual cleanup.

Fault types:

- Load generator concurrency increase.
- GreptimeDB latency or outage.
- Valkey latency or memory pressure.

## Approach 1: Chaos Mesh on Local Kubernetes

This approach runs the chaos suite in a local Kind cluster. Tracealyzer,
Valkey, GreptimeDB, load generation, and the scenario runner are deployed as
Kubernetes workloads. Chaos Mesh injects faults through Kubernetes custom
resources.

### High Level Design

Components:

- Kind cluster.
- Chaos Mesh controller and CRDs.
- Tracealyzer deployed through the existing Helm chart or a test-specific chart overlay.
- Valkey deployed as a Kubernetes workload.
- GreptimeDB deployed as a Kubernetes workload.
- Deterministic OTLP load generator deployed as a `Job` or long-running service.
- Optional demo load generator for soak-style scenarios.
- Chaos runner that applies scenario manifests, waits for phases, collects metrics, and asserts results.
- Prometheus scraping can be optional for local runs because the runner can read Tracealyzer `/metrics` directly.

Data flow:

```text
load-generator -> Tracealyzer OTLP endpoint
Tracealyzer -> Valkey service
Tracealyzer -> GreptimeDB ingester service
Tracealyzer -> GreptimeDB SQL service
chaos-runner -> Tracealyzer metrics endpoint
chaos-runner -> GreptimeDB SQL query endpoint
chaos-runner -> Kubernetes API / Chaos Mesh CRDs
```

Control plane:

- Scenario runner creates test data.
- Scenario runner applies a Chaos Mesh manifest such as `NetworkChaos`, `PodChaos`, `DNSChaos`, or `StressChaos`.
- Scenario runner waits for a declared duration or an observed metric condition.
- Scenario runner deletes or pauses the chaos object.
- Scenario runner validates Tracealyzer metrics and GreptimeDB rows.
- Scenario runner emits a human-readable scenario report.

Data generation:

- Use a deterministic OTLP generator for pass/fail assertions.
- Each generated trace should have known trace ID, span count, root count, breadth, depth, service count, operation count, and error count.
- Use the existing `test-stand/demo` load generator for longer soak and realism runs, where exact per-trace assertions are less important.

Suggested directory layout:

```text
chaos-test/
  README.md
  kind/
    cluster.yaml
  manifests/
    tracealyzer/
    valkey/
    greptime/
    load-generator/
  scenarios/
    greptime-network-partition.yaml
    valkey-network-partition.yaml
    tracealyzer-pod-kill.yaml
    valkey-memory-pressure.yaml
  runner/
```

Suggested Make targets:

```text
make chaos-kind-up
make chaos-install
make chaos-deploy
make chaos-test
make chaos-down
```

### Scenario Coverage

Covered well:

- GreptimeDB ingester unavailable through network partition, latency, loss, or pod kill.
- GreptimeDB SQL/readiness path unavailable.
- Valkey unavailable during startup, ingest, and sweep.
- Tracealyzer pod kill and restart.
- DNS failure for Kubernetes services.
- CPU and memory pressure on Tracealyzer or dependencies.
- Multi-step experiments using Chaos Mesh workflows or runner-controlled phases.

Partially covered:

- Valkey memory pressure. Chaos Mesh can add pressure to pods, but deterministic Valkey OOM behavior is better tested by configuring Valkey `maxmemory` and eviction policy.
- Disk I/O issues. Chaos Mesh has I/O chaos, but local Kind support can depend on the container runtime and storage setup.
- GreptimeDB application-level write errors. Chaos Mesh is better at infrastructure faults than returning specific GreptimeDB protocol errors.
- Exact mid-function timing such as "kill immediately after Valkey drain". This usually requires code instrumentation, a test hook, or a proxy/fake.

Not covered by Chaos Mesh alone:

- Validating business outcomes. Chaos Mesh injects faults, but the framework still needs a runner to assert metrics and database contents.
- Protocol-specific GreptimeDB responses such as specific gRPC status payloads.
- Precise Valkey command-level failures, for example failing `HSET` but allowing `ZADD`.

### Pros

- Closest to the production deployment model.
- Exercises Kubernetes readiness, restart, DNS, Services, pod lifecycle, and rollout behavior.
- Strong coverage for network and pod failures.
- Declarative chaos scenarios can be reviewed in code.
- Can later be reused in CI or pre-production clusters.
- Works well with existing Helm deployment patterns.

### Cons

- Higher local setup cost than Docker Compose.
- Requires Kind, kubectl, Helm, and Chaos Mesh installation.
- Slower feedback loop than unit tests or Compose.
- Debugging can involve several systems: Kubernetes, Chaos Mesh, Tracealyzer, Valkey, and GreptimeDB.
- Some low-level protocol failures still require a proxy, fake, or dependency-specific configuration.

## Approach 2: Home Made Docker Compose With Toxiproxy

This approach runs all components locally with Docker Compose. Toxiproxy sits
between Tracealyzer and its dependencies so the scenario runner can cut
connections, add latency, inject timeouts, and restore connectivity.

### High Level Design

Components:

- Docker Compose stack.
- Tracealyzer container built from the local repo.
- Valkey container.
- GreptimeDB container.
- Toxiproxy container.
- Deterministic OTLP load generator container.
- Optional existing `test-stand/demo` load generator and demo services.
- Chaos runner container or host-side Go command.

Data flow:

```text
load-generator -> Tracealyzer OTLP endpoint
Tracealyzer -> toxiproxy:valkey -> Valkey
Tracealyzer -> toxiproxy:greptime-ingester -> GreptimeDB
Tracealyzer -> toxiproxy:greptime-sql -> GreptimeDB
chaos-runner -> Toxiproxy admin API
chaos-runner -> Tracealyzer metrics endpoint
chaos-runner -> GreptimeDB SQL query endpoint
```

Control plane:

- Scenario runner starts from a clean Compose project.
- Scenario runner configures Toxiproxy routes.
- Scenario runner sends deterministic OTLP traces.
- Scenario runner applies proxy toxic changes, stops containers, restarts containers, or changes dependency configuration.
- Scenario runner polls metrics and GreptimeDB.
- Scenario runner tears the stack down with volumes removed for isolation.

Data generation:

- Use a deterministic OTLP generator for most assertions.
- Reuse `test-stand/demo` load generator for longer soak tests and realistic trace shapes.
- Keep deterministic scenarios small and fast so a developer can run them repeatedly.

Suggested directory layout:

```text
chaos-test/
  README.md
  compose/
    docker-compose.yaml
    tracealyzer.config.yaml
    toxiproxy.json
  scenarios/
    greptime-network-cut.yaml
    valkey-network-cut.yaml
    valkey-oom.yaml
    tracealyzer-kill.yaml
  runner/
```

Suggested Make targets:

```text
make chaos-compose-up
make chaos-compose-test
make chaos-compose-down
make chaos-compose-reset
```

### Scenario Coverage

Covered well:

- GreptimeDB ingester unavailable through Toxiproxy connection cut, timeout, latency, or bandwidth limit.
- GreptimeDB SQL path unavailable through a separate Toxiproxy route.
- Valkey unavailable through Toxiproxy.
- Valkey slow responses through Toxiproxy latency.
- Valkey memory pressure through explicit Valkey configuration.
- Tracealyzer process kill or restart through Docker Compose.
- Deterministic local data-loss and recovery experiments.

Partially covered:

- DNS failure. Compose can simulate some name resolution issues, but it is less natural than Kubernetes DNS chaos.
- Kubernetes readiness and rollout behavior. Compose can check HTTP endpoints, but it does not exercise Kubernetes probes or pod lifecycle.
- Pod-level resource pressure. Docker can constrain CPU and memory, but the behavior is not identical to Kubernetes scheduling and eviction.
- Production-like service discovery and network policy behavior.

Not covered well:

- Kubernetes Service, Endpoint, DNS, probe, and restart semantics.
- Chaos against a Helm-deployed Tracealyzer release.
- Kubernetes-specific graceful termination behavior.
- Multi-pod behavior, leaderless duplicate processing, or rollout interactions if Tracealyzer is ever scaled horizontally.

### Pros

- Easiest local developer setup.
- Fast feedback loop.
- No local Kubernetes cluster required.
- Toxiproxy gives precise dependency connection control.
- Straightforward logs and container lifecycle.
- Good fit for deterministic dependency failure tests.
- Can reuse the existing `test-stand` Docker Compose experience.

### Cons

- Less representative of production Kubernetes behavior.
- Requires custom scenario runner and Compose orchestration.
- Kubernetes-specific failures need a separate test layer later.
- Toxiproxy mainly simulates network faults, not pod lifecycle or control-plane behavior.
- Docker networking and service discovery differ from Kubernetes.

## Coverage Summary

| Scenario | Chaos Mesh on Kind | Docker Compose with Toxiproxy |
| --- | --- | --- |
| GreptimeDB ingester network outage | Strong | Strong |
| GreptimeDB SQL/readiness outage | Strong | Strong |
| GreptimeDB pod/container kill | Strong | Good |
| Valkey network outage | Strong | Strong |
| Valkey pod/container kill | Strong | Good |
| Valkey memory pressure/OOM | Partial, needs Valkey config | Strong with Valkey config |
| Tracealyzer pod kill | Strong | Good process-level equivalent |
| Graceful Kubernetes shutdown | Strong | Partial |
| DNS failure | Strong | Partial |
| Latency/loss/bandwidth | Strong | Strong |
| CPU/memory pressure | Strong | Partial |
| Kubernetes readiness/probes/rollout | Strong | Not covered |
| Protocol-specific Greptime errors | Partial | Partial, may need custom fake/proxy |
| Precise command-level Valkey failures | Not covered | Partial, may need custom fake/proxy |
| Deterministic local developer loop | Good | Strong |

## Recommendation

Use Chaos Mesh on Kind as the primary framework if the goal is to test
Tracealyzer as a Kubernetes service. It covers the most important production
failure modes and keeps the scenarios close to the deployment model.

Use Docker Compose with Toxiproxy as a secondary or bootstrap framework if the
team wants the fastest local feedback loop first. It is also useful for precise
dependency network manipulation and Valkey memory-pressure scenarios.

A practical phased plan:

1. Build a deterministic OTLP load generator and scenario runner that are not tied to either backend.
2. Implement the first scenarios with Docker Compose and Toxiproxy for speed.
3. Reuse the same runner assertions with Chaos Mesh on Kind.
4. Treat Chaos Mesh as the long-term canonical chaos environment once the local developer workflow is stable.

