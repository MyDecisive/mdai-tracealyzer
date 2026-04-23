package topology

// RootMetrics contains the per-root topology metrics emitted after trace
// finalization.
type RootMetrics struct {
	RootID          string
	TraceID         string
	RootService     string
	RootOperation   string
	Breadth         int32
	ServiceHopDepth int32
	ServiceCount    int32
	OperationCount  int32
	SpanCount       int32
	ErrorCount      int32
	RootDurationNS  int64
}
