package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// NewRegistry constructs the service's Prometheus registry with Go runtime
// and process collectors pre-registered. Subsystem packages register their
// own metrics against the returned registry as they come online.
//
// MetricsScheduler adds the /sched/* runtime metrics on top of the default
// set, exposing the goroutine-state breakdown (running/runnable/waiting) as
// the scrapeable complement to the on-demand goroutineleak profile.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsScheduler),
		),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}
