package buffer

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors returned from Put. Callers use errors.Is to classify
// without parsing driver-internal strings.
var (
	// ErrInvalidSpan covers records the buffer refuses to accept for
	// shape/encoding reasons (encoding failure, malformed identifier, etc.).
	ErrInvalidSpan = errors.New("invalid span")
	// ErrBufferFull covers backend-pressure rejections (Valkey OOM under
	// maxmemory with noeviction).
	ErrBufferFull = errors.New("buffer full")
	// ErrBackendUnavailable covers any other backend failure (connection
	// reset, timeout, protocol error).
	ErrBackendUnavailable = errors.New("backend unavailable")
)

type SpanRecord struct {
	TraceID      [16]byte `json:"trace_id"`
	SpanID       [8]byte  `json:"span_id"`
	ParentSpanID [8]byte  `json:"parent_span_id"`
	Service      string   `json:"service"`
	Name         string   `json:"name"`
	Kind         int32    `json:"kind"`
	StartTimeNs  int64    `json:"start_time_ns"`
	EndTimeNs    int64    `json:"end_time_ns"`
	StatusError  bool     `json:"status_error"`
	// OpAttrs is populated only for root spans (empty ParentSpanID); child
	// spans leave it nil to save buffer memory.
	OpAttrs map[string]string `json:"op_attrs,omitempty"`
}

// Finalizable pairs a trace with the trigger that selected it. When both
// fire on the same trace, "max_ttl" wins.
type Finalizable struct {
	TraceID [16]byte
	Trigger string
}

const (
	TriggerQuiet  = "quiet"
	TriggerMaxTTL = "max_ttl"
)

type Buffer interface {
	Put(ctx context.Context, r SpanRecord) error

	// Scan returns trace IDs whose last-write timestamp is older than
	// quietCutoff or whose first-seen timestamp is older than ttlCutoff.
	Scan(ctx context.Context, quietCutoff, ttlCutoff time.Time) ([]Finalizable, error)

	// Drain atomically fetches every span for a trace and removes the
	// trace's state from the buffer. The returned map is keyed by the
	// hex-encoded span ID.
	Drain(ctx context.Context, traceID [16]byte) (map[string]SpanRecord, error)

	Close()
}
