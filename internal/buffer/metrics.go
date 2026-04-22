package buffer

import "github.com/prometheus/client_golang/prometheus"

// RejectReason labels the topology_buffer_rejected_total counter.
type RejectReason string

const (
	ReasonOverflow     RejectReason = "overflow"
	ReasonBackendError RejectReason = "backend_error"
)

// Metrics is nil-safe; methods no-op on a nil receiver.
type Metrics struct {
	rejectedOverflow prometheus.Counter
	rejectedBackend  prometheus.Counter
	decodeErrors     prometheus.Counter
}

// NewMetrics registers buffer-local metrics on reg. The drain decode-error
// counter lives on a CounterVec owned by the ingest package (shared
// topology_spans_malformed_total), so it is threaded in pre-curried.
// decodeErrors may be nil; incDecodeErrors is a no-op in that case.
func NewMetrics(reg prometheus.Registerer, decodeErrors prometheus.Counter) *Metrics {
	rejected := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "topology_buffer_rejected_total",
		Help: "Span writes rejected by the Valkey buffer. reason=\"overflow\" is Valkey refusing the write under maxmemory pressure; reason=\"backend_error\" covers all other backend failures (connection reset, timeout, protocol error). Non-zero indicates spans are being dropped before reaching computation.",
	}, []string{"reason"})
	reg.MustRegister(rejected)

	return &Metrics{
		rejectedOverflow: rejected.WithLabelValues(string(ReasonOverflow)),
		rejectedBackend:  rejected.WithLabelValues(string(ReasonBackendError)),
		decodeErrors:     decodeErrors,
	}
}

func (m *Metrics) incRejected(reason RejectReason) {
	if m == nil {
		return
	}
	if reason == ReasonOverflow {
		m.rejectedOverflow.Inc()
		return
	}
	// Unknown reason falls through to backend_error; callers are internal
	// and pass one of the two defined constants.
	m.rejectedBackend.Inc()
}

func (m *Metrics) incDecodeErrors() {
	if m == nil || m.decodeErrors == nil {
		return
	}
	m.decodeErrors.Inc()
}
