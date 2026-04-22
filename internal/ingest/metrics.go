package ingest

import "github.com/prometheus/client_golang/prometheus"

// Metrics is nil-safe; methods no-op on a nil receiver.
type Metrics struct {
	spansReceived        prometheus.Counter
	spansMalformedIngest prometheus.Counter
	spansMalformedDrain  prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	received := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "topology_spans_received_total",
		Help: "Spans received by the ingest pipeline after OTLP decoding, including spans subsequently rejected by the buffer (tracked separately as topology_buffer_rejected_total).",
	})
	malformed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "topology_spans_malformed_total",
		Help: "Spans dropped because they could not be parsed. stage=\"ingest\" covers OTLP normalization failures (trace_id not 16 bytes, span_id not 8 bytes, parent_span_id wrong length, timestamps overflowing int64); stage=\"drain\" covers records that could not be decoded out of the Valkey buffer. A non-zero ingest count indicates a non-conformant upstream sender; a non-zero drain count indicates on-disk corruption or an encoding-format regression.",
	}, []string{"stage"})
	reg.MustRegister(received, malformed)

	return &Metrics{
		spansReceived:        received,
		spansMalformedIngest: malformed.WithLabelValues("ingest"),
		spansMalformedDrain:  malformed.WithLabelValues("drain"),
	}
}

// MalformedDrainCounter returns the stage="drain" counter so the buffer
// package can bump it from Drain without depending on ingest internals.
// Returns nil on a nil receiver.
//
//nolint:ireturn // prometheus.Counter is an interface by design; the caller stores it as one.
func (m *Metrics) MalformedDrainCounter() prometheus.Counter {
	if m == nil {
		return nil
	}
	return m.spansMalformedDrain
}

func (m *Metrics) incSpansReceived(n int) {
	if m == nil || n <= 0 {
		return
	}
	m.spansReceived.Add(float64(n))
}

func (m *Metrics) incSpansMalformed(n int) {
	if m == nil || n <= 0 {
		return
	}
	m.spansMalformedIngest.Add(float64(n))
}
