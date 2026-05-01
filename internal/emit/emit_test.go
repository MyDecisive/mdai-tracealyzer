package emit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// startEmitter runs e.Start in a goroutine bound to the test lifetime
// and registers a t.Cleanup that cancels Start, waits for it to return,
// then invokes Stop with a 1s grace. The Stop error is reported via
// t.Errorf so cleanup failures fail the test without each caller having
// to remember to check.
func startEmitter(t *testing.T, e *Emitter) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		_ = e.Start(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		if err := e.Stop(stopCtx); err != nil {
			t.Errorf("emitter Stop: %v", err)
		}
	})
}

type fakeWriter struct {
	mu       sync.Mutex
	batches  []writeBatch
	errs     []error
	callCh   chan struct{}
	closeErr error
	closed   bool
}

func (w *fakeWriter) Write(_ context.Context, batch writeBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.batches = append(w.batches, batch)
	if w.callCh != nil {
		select {
		case w.callCh <- struct{}{}:
		default:
		}
	}
	if len(w.errs) == 0 {
		return nil
	}
	err := w.errs[0]
	w.errs = w.errs[1:]
	return err
}

func (*fakeWriter) HealthCheck(_ context.Context) error { return nil }

func (w *fakeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return w.closeErr
}

func (w *fakeWriter) batchCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.batches)
}

func (w *fakeWriter) lastBatch() writeBatch {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.batches[len(w.batches)-1]
}

func TestEmitterFlushesOnBatchSize(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	writer := &fakeWriter{callCh: make(chan struct{}, 1)}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	if emitErr := e.Emit(t.Context(), sampleRows(2)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	waitForCalls(t, writer.callCh, 1)

	if got := writer.batchCount(); got != 1 {
		t.Fatalf("want 1 batch, got %d", got)
	}

	batch := writer.lastBatch()
	if batch.Table != "trace_root_topology" {
		t.Fatalf("want table trace_root_topology, got %q", batch.Table)
	}
	if len(batch.Rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(batch.Rows))
	}
	wantTS := fixedNow()()
	if !batch.Rows[0].Timestamp.Equal(wantTS) {
		t.Fatalf("want timestamp %v, got %v", wantTS, batch.Rows[0].Timestamp)
	}
}

func TestEmitterFlushesOnTimer(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	cfg := testEmitterConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = config.Duration(5 * time.Millisecond)

	writer := &fakeWriter{callCh: make(chan struct{}, 1)}
	e := newWithWriter(cfg, zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	if emitErr := e.Emit(t.Context(), sampleRows(1)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	waitForCalls(t, writer.callCh, 1)

	if got := writer.batchCount(); got != 1 {
		t.Fatalf("want 1 batch, got %d", got)
	}
}

func TestEmitterRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	writer := &fakeWriter{
		callCh: make(chan struct{}, 3),
		errs:   []error{errors.New("temporary"), errors.New("temporary")},
	}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	if emitErr := e.Emit(t.Context(), sampleRows(2)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	waitForCalls(t, writer.callCh, 3)

	if got := writer.batchCount(); got != 3 {
		t.Fatalf("want 3 write attempts, got %d", got)
	}
	if got := testutil.ToFloat64(m.emissionsFailed); got != 0 {
		t.Fatalf("want no dropped rows, got %v", got)
	}
}

func TestEmitterReturnsErrQueueFullAndCountsDrops(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	cfg := testEmitterConfig()
	cfg.QueueCapacity = 1
	cfg.BatchSize = 10
	cfg.FlushInterval = config.Duration(time.Hour)

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	writer := &fakeWriter{}
	e := newWithWriter(cfg, logger, m, writer, fixedNow())
	// Intentionally do NOT start the worker so the queue does not drain.

	if emitErr := e.Emit(t.Context(), sampleRows(1)); emitErr != nil {
		t.Fatalf("first Emit: %v", emitErr)
	}

	err = e.Emit(t.Context(), sampleRows(2))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	if got := testutil.ToFloat64(m.emissionsFailed); got != 2 {
		t.Fatalf("want 2 dropped rows, got %v", got)
	}
	if logs.Len() == 0 {
		t.Fatal("expected warning log for dropped rows")
	}
}

func TestEmitterStopFlushesPendingRows(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	cfg := testEmitterConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = config.Duration(time.Hour)

	writer := &fakeWriter{}
	e := newWithWriter(cfg, zap.NewNop(), m, writer, fixedNow())
	// No Start: rows sit on the queue and Stop must drain + flush them.

	if emitErr := e.Emit(t.Context(), sampleRows(1)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if stopErr := e.Stop(stopCtx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	if got := writer.batchCount(); got != 1 {
		t.Fatalf("want 1 batch on stop, got %d", got)
	}
	if !writer.closed {
		t.Fatal("writer was not closed")
	}
}

func TestEmitterEmitAfterStopReturnsErrClosed(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, &fakeWriter{}, fixedNow())
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if stopErr := e.Stop(stopCtx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	err = e.Emit(t.Context(), sampleRows(1))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestEmitterStopReturnsWriteError(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	writer := &fakeWriter{
		errs: []error{
			errors.New("write failed"),
			errors.New("write failed"),
			errors.New("write failed"),
		},
	}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, writer, fixedNow())

	if emitErr := e.Emit(t.Context(), sampleRows(2)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = e.Stop(stopCtx)
	if err == nil {
		t.Fatal("expected Stop to return write error from final flush")
	}
	if !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMakeWriteBatchMapsAllTopologyFields(t *testing.T) {
	t.Parallel()

	ts := time.Unix(1700000100, 456)
	rows := []topology.RootMetrics{{
		RootID:          "svc::root-op",
		TraceID:         "001122",
		RootService:     "svc",
		RootOperation:   "root-op",
		Breadth:         11,
		ServiceHopDepth: 12,
		ServiceCount:    13,
		OperationCount:  14,
		SpanCount:       15,
		ErrorCount:      16,
		RootDurationNS:  17,
	}}

	batch := makeWriteBatch(rows, ts)
	if batch.Table != "trace_root_topology" {
		t.Fatalf("want table trace_root_topology, got %q", batch.Table)
	}
	if len(batch.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(batch.Rows))
	}

	got := batch.Rows[0]
	if !got.Timestamp.Equal(ts) {
		t.Fatalf("want timestamp %v, got %v", ts, got.Timestamp)
	}
	if got.RootID != rows[0].RootID {
		t.Fatalf("want root_id %q, got %q", rows[0].RootID, got.RootID)
	}
	if got.TraceID != rows[0].TraceID {
		t.Fatalf("want trace_id %q, got %q", rows[0].TraceID, got.TraceID)
	}
	if got.RootService != rows[0].RootService {
		t.Fatalf("want root_service %q, got %q", rows[0].RootService, got.RootService)
	}
	if got.RootOperation != rows[0].RootOperation {
		t.Fatalf("want root_operation %q, got %q", rows[0].RootOperation, got.RootOperation)
	}
	if got.Breadth != rows[0].Breadth {
		t.Fatalf("want breadth %d, got %d", rows[0].Breadth, got.Breadth)
	}
	if got.ServiceHopDepth != rows[0].ServiceHopDepth {
		t.Fatalf("want service_hop_depth %d, got %d", rows[0].ServiceHopDepth, got.ServiceHopDepth)
	}
	if got.ServiceCount != rows[0].ServiceCount {
		t.Fatalf("want service_count %d, got %d", rows[0].ServiceCount, got.ServiceCount)
	}
	if got.OperationCount != rows[0].OperationCount {
		t.Fatalf("want operation_count %d, got %d", rows[0].OperationCount, got.OperationCount)
	}
	if got.SpanCount != rows[0].SpanCount {
		t.Fatalf("want span_count %d, got %d", rows[0].SpanCount, got.SpanCount)
	}
	if got.ErrorCount != rows[0].ErrorCount {
		t.Fatalf("want error_count %d, got %d", rows[0].ErrorCount, got.ErrorCount)
	}
	if got.RootDurationNS != rows[0].RootDurationNS {
		t.Fatalf("want root_duration_ns %d, got %d", rows[0].RootDurationNS, got.RootDurationNS)
	}
}

func sampleRows(n int) []topology.RootMetrics {
	rows := make([]topology.RootMetrics, 0, n)
	for i := range n {
		rows = append(rows, topology.RootMetrics{
			RootID:          "svc::op",
			TraceID:         "trace-" + string(rune('a'+i)),
			RootService:     "svc",
			RootOperation:   "op",
			Breadth:         1,
			ServiceHopDepth: 2,
			ServiceCount:    3,
			OperationCount:  4,
			SpanCount:       5,
			ErrorCount:      6,
			RootDurationNS:  7,
		})
	}
	return rows
}

func testEmitterConfig() config.Emitter {
	return config.Emitter{
		TableTTL:       "14d",
		Timeout:        config.Duration(time.Second),
		MaxRetries:     2,
		InitialBackoff: config.Duration(time.Millisecond),
		BatchSize:      2,
		FlushInterval:  config.Duration(time.Hour),
		QueueCapacity:  8,
	}
}

func fixedNow() func() time.Time {
	ts := time.Unix(1700000000, 123)
	return func() time.Time { return ts }
}

func waitForCalls(t *testing.T, ch <-chan struct{}, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	for range want {
		select {
		case <-ch:
		case <-deadline:
			t.Fatal("timed out waiting for write call")
		}
	}
}
