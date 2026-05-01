package sweep

import (
	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
)

// TopologyComputer is the production Computer for the sweeper. It adapts
// buffer.SpanRecord values into topology.Span before delegating to
// topology.Compute, and surfaces ErrNoRoot when a trace has no
// discoverable root span. Multi-root traces yield one row per root;
// orphans propagate even on the ErrNoRoot branch so the sweeper records
// them for all-orphan traces.
type TopologyComputer struct{}

var _ Computer = TopologyComputer{}

func (TopologyComputer) Compute(traceID [16]byte, _ string, records map[string]buffer.SpanRecord) ([]topology.RootMetrics, int32, error) {
	spans := make(map[[8]byte]topology.Span, len(records))
	for _, r := range records {
		spans[r.SpanID] = topology.Span{
			SpanID:       r.SpanID,
			ParentSpanID: r.ParentSpanID,
			Service:      r.Service,
			Name:         r.Name,
			Kind:         r.Kind,
			StartTimeNs:  r.StartTimeNs,
			EndTimeNs:    r.EndTimeNs,
			StatusError:  r.StatusError,
			OpAttrs:      r.OpAttrs,
		}
	}
	rows, orphans := topology.Compute(traceID, spans)
	if len(rows) == 0 && len(spans) > 0 {
		return nil, orphans, ErrNoRoot
	}
	if len(rows) == 0 {
		return nil, orphans, nil
	}
	return rows, orphans, nil
}
