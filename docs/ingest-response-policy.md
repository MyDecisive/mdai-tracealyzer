# Ingest response policy

This document is the contract for how the OTLP/gRPC and OTLP/HTTP trace
ingest handlers translate per-batch outcomes into client responses. The
two handlers (`internal/ingest/grpc.go`, `internal/ingest/http.go`) and
the shared classifier (`internal/ingest/recorder.go`) implement this
policy. The document and the implementation are kept in sync.

## Policy

For each batch the handler receives, the response is decided by the
combined outcome of two phases:

- **Normalization** (`Normalize`) drops malformed spans before they
  reach the recorder. Dropped spans are counted as `malformed`.
- **Recording** (`Recorder.Put` per surviving span) returns one of two
  error classes for each rejection:
  - *Transient*: `buffer.ErrBufferFull` or `buffer.ErrBackendUnavailable`.
  - *Permanent*: `buffer.ErrInvalidSpan` or any other error.

The response is then:

| Batch outcome | Response |
|---|---|
| Any transient rejection (alone or mixed with permanent / malformed) | **HTTP 503** / gRPC `Unavailable` |
| Permanent and/or malformed only, no transient | **HTTP 200** / gRPC `OK` with `PartialSuccess { rejected_spans = permanent + malformed }` |
| No rejections | **HTTP 200** / gRPC `OK` with empty `ExportTraceServiceResponse` |

`ErrorMessage` on `PartialSuccess` is the classification of the first
permanent error seen, or `"spans dropped at normalization"` when only
malformed spans were rejected.

The OTLP/HTTP 503 response body is a protobuf-encoded
`google.rpc.Status` carrying `code = Unavailable` and `message =`
classification of the transient error, served with
`Content-Type: application/x-protobuf`. This is the form the OTLP
spec requires for all 4xx/5xx HTTP responses. The gRPC side returns
`status.Error(codes.Unavailable, …)` natively.

## Invariants

The policy reduces to five mechanical checks. Any change to the
ingest code must preserve all of them.

- **I1.** `PartialSuccess` MUST only represent permanent or malformed
  rejection. Transient rejections never appear in a `PartialSuccess`
  response.
- **I2.** Any transient recorder rejection MUST make the whole
  request retryable (HTTP 503 / gRPC `Unavailable`).
- **I3.** A mixed transient + permanent batch MUST return the
  retryable response. I2 wins over I1 by construction.
- **I4.** HTTP retryable responses MUST carry a protobuf
  `google.rpc.Status` body, served as
  `Content-Type: application/x-protobuf`.
- **I5.** gRPC retryable responses MUST use `codes.Unavailable`.

## Rationale

The OTLP spec defines three response paths but is silent on batches
containing both retryable and non-retryable rejections. The two
choices in that gap have different consequences:

- **PartialSuccess on a mixed batch.** OTLP clients MUST NOT retry
  rejected spans on a PartialSuccess response. A batch that contains
  a span rejected for `ErrBufferFull` (transient), reported via
  PartialSuccess, silently drops a valid span the client could have
  recovered by retrying.
- **503 on a mixed batch.** The client retries the whole batch.
  Permanent rejections fail again on each retry, but the repeated
  retries are bounded by the lifetime of the transient condition:
  once the buffer recovers, the next response is PartialSuccess (no
  transient remaining), the client stops retrying, and the permanent
  rejections surface to the client exactly once.

The chosen path is the second. The asymmetry is intentional:
permanent rejections are durable signals that survive retries;
transient rejections are not, so the response code carries the
protocol's retry signal for them.

## OTLP specification basis

This policy is anchored to OTLP specification version **v1.10.0**
([`opentelemetry-proto/docs/specification.md` @ v1.10.0](https://github.com/open-telemetry/opentelemetry-proto/blob/v1.10.0/docs/specification.md)):

- *Partial Success:* the server MUST respond with HTTP 200 OK, and the
  client MUST NOT retry the request when `partial_success` is
  populated.
- *Retryable errors:* the client SHOULD record the error and may retry
  the same data.
- *Non-retryable errors:* the client MUST NOT retry.

The spec does **not** prescribe behaviour for mixed transient and
permanent rejections within a single batch. This policy fills that
gap explicitly.

## Divergence from otelcol

The upstream OTLP receiver in `open-telemetry/opentelemetry-collector`
(`receiver/otlpreceiver/internal/trace/otlp.go`) does **not** use
`PartialSuccess`. It returns the consumer's error wholesale through
`receiver/otlpreceiver/internal/errors/errors.go`'s status mapping:

- If the consumer's error already carries a gRPC status, that status
  is used.
- Otherwise: non-permanent → `codes.Unavailable` (HTTP 503),
  permanent → `codes.Internal` (HTTP 500). The HTTP 400 case fires
  only when a consumer explicitly returns a status-coded
  `InvalidArgument`.

That pattern is also spec-compliant. Tracealyzer diverges deliberately:
the recorder operates per-span (`Recorder.Put` per record, not a
batch-level consumer), so a single permanent rejection in a batch of
otherwise-accepted spans does not cause the client to drop the whole
batch. `PartialSuccess` is the protocol's mechanism for exactly that
case.

Operators familiar with otelcol's no-`PartialSuccess` behaviour
receive 200-with-`PartialSuccess` for per-span permanent rejections
on this service instead.
