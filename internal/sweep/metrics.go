package sweep

import (
	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
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
	sweepsOK             prometheus.Counter
	sweepsScanError      prometheus.Counter
	sweepsEmitError      prometheus.Counter
	finalized            prometheus.Counter
	triggerQuiet         prometheus.Counter
	triggerMaxTTL        prometheus.Counter
	drainErrors          prometheus.Counter
	computeErrors        prometheus.Counter
	computeSkippedNoRoot prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
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
		Help: "Compute failures other than ErrNoRoot. A non-zero value is a bug in the topology computation for a specific trace shape; the trace is dropped after its buffer state was removed.",
	})
	computeSkipped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "topology_compute_skipped_total",
		Help: "Expected skips during compute. reason=\"no_root\" is a trace with no discoverable root span (e.g. an orphan flushed by max_ttl before the root arrived).",
	}, []string{"reason"})
	reg.MustRegister(sweeps, finalized, trigger, drain, compute, computeSkipped)

	return &Metrics{
		sweepsOK:             sweeps.WithLabelValues(resultOK),
		sweepsScanError:      sweeps.WithLabelValues(resultScanError),
		sweepsEmitError:      sweeps.WithLabelValues(resultEmitError),
		finalized:            finalized,
		triggerQuiet:         trigger.WithLabelValues(buffer.TriggerQuiet),
		triggerMaxTTL:        trigger.WithLabelValues(buffer.TriggerMaxTTL),
		drainErrors:          drain,
		computeErrors:        compute,
		computeSkippedNoRoot: computeSkipped.WithLabelValues(reasonNoRoot),
	}
}

func (m *Metrics) incSweep(result string) {
	if m == nil {
		return
	}
	switch result {
	case resultOK:
		m.sweepsOK.Inc()
	case resultScanError:
		m.sweepsScanError.Inc()
	case resultEmitError:
		m.sweepsEmitError.Inc()
	default:
		// Callers are internal and pass one of the three defined constants.
	}
}

func (m *Metrics) incFinalized(trigger string) {
	if m == nil {
		return
	}
	m.finalized.Inc()
	switch trigger {
	case buffer.TriggerQuiet:
		m.triggerQuiet.Inc()
	case buffer.TriggerMaxTTL:
		m.triggerMaxTTL.Inc()
	default:
		// Callers pass one of the two defined trigger constants.
	}
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
	if reason == reasonNoRoot {
		m.computeSkippedNoRoot.Inc()
	}
}
