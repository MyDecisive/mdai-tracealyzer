package sweep

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	resultOK        = "ok"
	resultScanError = "scan_error"
	resultEmitError = "emit_error"
	reasonNoRoot    = "no_root"
)

// Metrics is nil-safe; methods no-op on a nil receiver.
type Metrics struct {
	sweeps          *prometheus.CounterVec
	trigger         *prometheus.CounterVec
	computeSkipped  *prometheus.CounterVec
	finalized       prometheus.Counter
	drainErrors     prometheus.Counter
	computeErrors   prometheus.Counter
	orphanSpans     prometheus.Counter
	computeDuration prometheus.Histogram
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		return nil
	}
	sweeps := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "topology_sweeps_total",
		Help: "Sweep ticks, partitioned by outcome. result=\"ok\" is a clean tick (including ticks that found nothing finalizable); result=\"scan_error\" is a Valkey Scan failure; result=\"emit_error\" is an Emit failure for the tick's batch. Per-trace drain/compute failures stay inside a tick and do not change this label — see topology_drain_errors_total and topology_compute_errors_total.",
	}, []string{"result"})
	finalized := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "topology_traces_finalized_total",
		Help: "Traces finalized and enqueued for emission, one increment per root. Does not imply a successful GreptimeDB write — see topology_emissions_failed_total.",
	})
	trigger := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "topology_finalization_trigger_total",
		Help: "Finalization trigger mix. trigger=\"quiet\" caught the trace via the quiet-period cutoff; trigger=\"max_ttl\" caught it via the hard TTL. A rising max_ttl share indicates traces are living longer than quiet_period can cover.",
	}, []string{"trigger"})
	drain := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "topology_drain_errors_total",
		Help: "Per-trace Drain failures during a sweep tick. The trace is skipped and retried on the next tick; persistent growth indicates a Valkey backend problem.",
	})
	compute := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "topology_compute_errors_total",
		Help: "Compute failures other than ErrNoRoot. A non-zero value is a bug in the topology computation for a specific trace shape; the trace is dropped after its buffer state was removed. See topology_emissions_failed_total for drops that happen after successful Compute.",
	})
	computeSkipped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "topology_compute_skipped_total",
		Help: "Expected skips during compute. reason=\"no_root\" is a trace with no discoverable root span (e.g. an orphan flushed by max_ttl before the root arrived).",
	}, []string{"reason"})
	orphan := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "topology_orphan_spans_total",
		Help: "Spans dropped at reconstruction because they were not reachable from any authentic root. Rising values indicate pathological truncation upstream.",
	})
	computeDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "topology_compute_duration_seconds",
		Help: "Per-trace topology computation time, measured around a single Compute call.",
	})
	reg.MustRegister(sweeps, finalized, trigger, drain, compute, computeSkipped, orphan, computeDuration)

	return &Metrics{
		sweeps:          sweeps,
		trigger:         trigger,
		computeSkipped:  computeSkipped,
		finalized:       finalized,
		drainErrors:     drain,
		computeErrors:   compute,
		orphanSpans:     orphan,
		computeDuration: computeDuration,
	}
}

func (m *Metrics) incSweep(result string) {
	if m == nil {
		return
	}
	m.sweeps.WithLabelValues(result).Inc()
}

func (m *Metrics) incFinalized(trigger string) {
	if m == nil {
		return
	}
	m.finalized.Inc()
	m.trigger.WithLabelValues(trigger).Inc()
}

func (m *Metrics) incDrainError() {
	if m == nil {
		return
	}
	m.drainErrors.Inc()
}

func (m *Metrics) incComputeError() {
	if m == nil {
		return
	}
	m.computeErrors.Inc()
}

func (m *Metrics) incComputeSkipped(reason string) {
	if m == nil {
		return
	}
	m.computeSkipped.WithLabelValues(reason).Inc()
}

func (m *Metrics) addOrphanSpans(n int32) {
	if m == nil || n <= 0 {
		return
	}
	m.orphanSpans.Add(float64(n))
}

func (m *Metrics) observeComputeDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.computeDuration.Observe(d.Seconds())
}
