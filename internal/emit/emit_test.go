package emit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type noopHost struct{}

func (noopHost) Fatal(string, error) {}

// startEmitter calls e.Start (which is non-blocking) and registers a
// t.Cleanup that calls Shutdown with a 1s grace. The Shutdown error is
// reported via t.Errorf so cleanup failures fail the test.
func startEmitter(t *testing.T, e *Emitter) {
	t.Helper()
	if err := e.Start(t.Context(), noopHost{}); err != nil {
		t.Fatalf("emitter Start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		if err := e.Shutdown(stopCtx); err != nil {
			t.Errorf("emitter Shutdown: %v", err)
		}
	})
}

type fakeWriter struct {
	mu         sync.Mutex
	batches    []writeBatch
	errs       []error
	callCh     chan struct{}
	closeErr   error
	closed     bool
	closeCount int
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
	w.closeCount++
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

func TestEmitter_ShutdownWithoutStartClosesWriter(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	w := &fakeWriter{}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, w, fixedNow())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown without Start: %v", err)
	}
	if !w.closed {
		t.Fatal("writer.Close not called when Shutdown is invoked without prior Start")
	}
	if err := e.Shutdown(ctx); err != nil {
		t.Errorf("second Shutdown must be idempotent, got %v", err)
	}
	if got := w.closeCount; got != 1 {
		t.Errorf("writer.Close called %d times; want 1", got)
	}
}

func TestEmitterShutdownFlushesPendingRows(t *testing.T) {
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
	startEmitter(t, e)

	if emitErr := e.Emit(t.Context(), sampleRows(1)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if stopErr := e.Shutdown(stopCtx); stopErr != nil {
		t.Fatalf("Shutdown: %v", stopErr)
	}

	if got := writer.batchCount(); got != 1 {
		t.Fatalf("want 1 batch on stop, got %d", got)
	}
	if !writer.closed {
		t.Fatal("writer was not closed")
	}
}

func TestEmitterEmitAfterShutdownReturnsErrClosed(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, &fakeWriter{}, fixedNow())
	startEmitter(t, e)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if stopErr := e.Shutdown(stopCtx); stopErr != nil {
		t.Fatalf("Shutdown: %v", stopErr)
	}

	err = e.Emit(t.Context(), sampleRows(1))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestEmitterShutdownReturnsWriteError(t *testing.T) {
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
	cfg := testEmitterConfig()
	cfg.BatchSize = 5
	e := newWithWriter(cfg, zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	if emitErr := e.Emit(t.Context(), sampleRows(2)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = e.Shutdown(stopCtx)
	if err == nil {
		t.Fatal("expected Shutdown to return write error from final flush")
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

func TestEmitter_EmitAfterShutdownBumpsEmissionsFailed(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, &fakeWriter{}, fixedNow())
	startEmitter(t, e)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if stopErr := e.Shutdown(stopCtx); stopErr != nil {
		t.Fatalf("Shutdown: %v", stopErr)
	}

	rows := sampleRows(3)
	if emitErr := e.Emit(context.Background(), rows); !errors.Is(emitErr, ErrClosed) {
		t.Fatalf("Emit: want ErrClosed, got %v", emitErr)
	}

	got := testutil.ToFloat64(m.emissionsFailed)
	if got != float64(len(rows)) {
		t.Fatalf("topology_emissions_failed_total: got %v, want %d", got, len(rows))
	}
}

func TestEmitter_ShutdownRejectsConcurrentEmitWithoutSilentLoss(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	w := &fakeWriter{}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, w, time.Now)
	startEmitter(t, e)

	e.closedMu.RLock()

	stopErr := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopErr <- e.Shutdown(stopCtx)
	}()

	select {
	case err := <-stopErr:
		t.Fatalf("Shutdown returned %v while RLock was held; should have blocked on Lock", err)
	case <-time.After(50 * time.Millisecond):
	}

	e.queue <- sampleRows(1)
	e.closedMu.RUnlock()

	if err := <-stopErr; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := w.batchCount(); got != 1 {
		t.Fatalf("writer.batchCount = %d, want 1 (row queued before closed=true must reach the writer)", got)
	}

	if emitErr := e.Emit(context.Background(), sampleRows(1)); !errors.Is(emitErr, ErrClosed) {
		t.Fatalf("post-Shutdown Emit: want ErrClosed, got %v", emitErr)
	}
}

type slowWriter struct {
	mu    sync.Mutex
	calls int
	delay time.Duration
}

func (w *slowWriter) Write(_ context.Context, _ writeBatch) error {
	w.mu.Lock()
	w.calls++
	w.mu.Unlock()
	time.Sleep(w.delay)
	return nil
}
func (*slowWriter) HealthCheck(_ context.Context) error { return nil }
func (*slowWriter) Close() error                        { return nil }

func TestEmitter_ShutdownDuringInFlightWriteIsSafe(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	cfg := config.Emitter{
		TableTTL:       "14d",
		Timeout:        config.Duration(time.Second),
		MaxRetries:     0,
		InitialBackoff: config.Duration(time.Millisecond),
		BatchSize:      1,
		FlushInterval:  config.Duration(time.Hour),
		QueueCapacity:  64,
	}
	w := &slowWriter{delay: 30 * time.Millisecond}
	e := newWithWriter(cfg, zap.NewNop(), m, w, time.Now)

	if err := e.Start(t.Context(), noopHost{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if emitErr := e.Emit(t.Context(), sampleRows(20)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	time.Sleep(50 * time.Millisecond)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if stopErr := e.Shutdown(stopCtx); stopErr != nil {
		t.Fatalf("Shutdown: %v", stopErr)
	}
}

type hangingWriter struct {
	entered    chan struct{}
	release    chan struct{}
	closed     atomic.Bool
	writeCalls atomic.Int32
	closeCalls atomic.Int32
}

func (w *hangingWriter) Write(ctx context.Context, _ writeBatch) error {
	w.writeCalls.Add(1)
	select {
	case w.entered <- struct{}{}:
	default:
	}
	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*hangingWriter) HealthCheck(_ context.Context) error { return nil }

func (w *hangingWriter) Close() error {
	w.closed.Store(true)
	w.closeCalls.Add(1)
	return nil
}

func TestEmitter_ShutdownClosesWriterWhenContextExpiresMidWrite(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	cfg := config.Emitter{
		TableTTL:       "14d",
		Timeout:        config.Duration(time.Hour),
		MaxRetries:     0,
		InitialBackoff: config.Duration(time.Millisecond),
		BatchSize:      1,
		FlushInterval:  config.Duration(time.Hour),
		QueueCapacity:  4,
	}

	w := &hangingWriter{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(w.release) })

	e := newWithWriter(cfg, zap.NewNop(), m, w, time.Now)
	if err := e.Start(t.Context(), noopHost{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if emitErr := e.Emit(context.Background(), sampleRows(1)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer.Write was never called; cannot exercise stuck-write path")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()
	stopErr := e.Shutdown(stopCtx)

	if stopErr == nil {
		t.Fatal("Shutdown returned nil when ctx expired mid-write; want ctx error")
	}
	if !errors.Is(stopErr, context.DeadlineExceeded) && !errors.Is(stopErr, context.Canceled) {
		t.Fatalf("Shutdown error = %v; want context.DeadlineExceeded or context.Canceled", stopErr)
	}
	if !w.closed.Load() {
		t.Fatal("writer.Close() was not called: Shutdown returned with ctx expired and leaked the writer")
	}
	if got := w.writeCalls.Load(); got != 1 {
		t.Fatalf("writer.Write call count = %d; want 1 (no extra final flush with already-canceled ctx)", got)
	}
}

func TestEmitter_ShutdownGraceExpiryRecordsQueuedRowsAsDropped(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	cfg := config.Emitter{
		TableTTL:       "14d",
		Timeout:        config.Duration(time.Hour),
		MaxRetries:     0,
		InitialBackoff: config.Duration(time.Millisecond),
		BatchSize:      1,
		FlushInterval:  config.Duration(time.Hour),
		QueueCapacity:  16,
	}

	w := &hangingWriter{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(w.release) })

	e := newWithWriter(cfg, zap.NewNop(), m, w, time.Now)
	if err := e.Start(t.Context(), noopHost{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if emitErr := e.Emit(context.Background(), sampleRows(1)); emitErr != nil {
		t.Fatalf("Emit (in-flight): %v", emitErr)
	}

	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer.Write was never called; cannot exercise stuck-write path")
	}

	const queuedRows = 4
	for range queuedRows {
		if emitErr := e.Emit(context.Background(), sampleRows(1)); emitErr != nil {
			t.Fatalf("Emit (queued): %v", emitErr)
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()
	stopErr := e.Shutdown(stopCtx)
	if stopErr == nil {
		t.Fatal("Shutdown returned nil when ctx expired; want ctx error")
	}
	if !errors.Is(stopErr, context.DeadlineExceeded) && !errors.Is(stopErr, context.Canceled) {
		t.Fatalf("Shutdown error = %v; want context.DeadlineExceeded or context.Canceled", stopErr)
	}

	wantDropped := float64(1 + queuedRows)
	if got := testutil.ToFloat64(m.emissionsFailed); got != wantDropped {
		t.Fatalf("topology_emissions_failed_total = %v; want %v (1 in-flight + %d queued)", got, wantDropped, queuedRows)
	}
}

func TestEmitter_ShutdownWithPreCancelledCtxReturnsCtxErr(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	w := &fakeWriter{}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, w, fixedNow())
	if err := e.Start(t.Context(), noopHost{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	stopCtx, stopCancel := context.WithCancel(context.Background())
	stopCancel()

	stopErr := e.Shutdown(stopCtx)
	if !errors.Is(stopErr, context.Canceled) {
		t.Fatalf("Shutdown error = %v; want context.Canceled", stopErr)
	}
	if !w.closed {
		t.Fatal("writer.Close was not called when Shutdown ran with pre-cancelled ctx")
	}
}

func TestEmitter_ShutdownAfterGraceExpiryIsIdempotent(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	cfg := config.Emitter{
		TableTTL:       "14d",
		Timeout:        config.Duration(time.Hour),
		MaxRetries:     0,
		InitialBackoff: config.Duration(time.Millisecond),
		BatchSize:      1,
		FlushInterval:  config.Duration(time.Hour),
		QueueCapacity:  4,
	}

	w := &hangingWriter{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(w.release) })

	e := newWithWriter(cfg, zap.NewNop(), m, w, time.Now)
	if err := e.Start(t.Context(), noopHost{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if emitErr := e.Emit(context.Background(), sampleRows(1)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer.Write was never called")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stopCancel()
	if err := e.Shutdown(stopCtx); err == nil {
		t.Fatal("first Shutdown returned nil; want ctx error")
	}

	dropsAfterFirst := testutil.ToFloat64(m.emissionsFailed)
	writesAfterFirst := w.writeCalls.Load()
	closesAfterFirst := w.closeCalls.Load()
	if closesAfterFirst != 1 {
		t.Fatalf("writer.Close calls after first Shutdown = %d; want 1", closesAfterFirst)
	}

	if err := e.Shutdown(stopCtx); err != nil {
		t.Fatalf("second Shutdown returned %v; want nil (idempotent)", err)
	}
	if got := testutil.ToFloat64(m.emissionsFailed); got != dropsAfterFirst {
		t.Fatalf("emissionsFailed after second Shutdown = %v; want %v (no double-count)", got, dropsAfterFirst)
	}
	if got := w.writeCalls.Load(); got != writesAfterFirst {
		t.Fatalf("writer.Write calls after second Shutdown = %d; want %d", got, writesAfterFirst)
	}
	if got := w.closeCalls.Load(); got != closesAfterFirst {
		t.Fatalf("writer.Close calls after second Shutdown = %d; want %d (close exactly once)", got, closesAfterFirst)
	}
}

func TestEmitter_RetryReusesBatchTimestamp(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	var calls atomic.Int32
	base := time.Unix(1_700_000_000, 0)
	nowFn := func() time.Time {
		n := calls.Add(1)
		return base.Add(time.Duration(n) * time.Millisecond)
	}

	writer := &fakeWriter{
		callCh: make(chan struct{}, 3),
		errs:   []error{errors.New("temp"), errors.New("temp")},
	}
	cfg := testEmitterConfig()
	cfg.MaxRetries = 5

	e := newWithWriter(cfg, zap.NewNop(), m, writer, nowFn)
	startEmitter(t, e)

	if emitErr := e.Emit(context.Background(), sampleRows(2)); emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	waitForCalls(t, writer.callCh, 3)

	if got := writer.batchCount(); got != 3 {
		t.Fatalf("write attempts: got %d, want 3 (2 failed + 1 success)", got)
	}

	writer.mu.Lock()
	batches := append([]writeBatch(nil), writer.batches...)
	writer.mu.Unlock()

	wantTS := batches[0].Rows[0].Timestamp
	for i, b := range batches {
		for j, r := range b.Rows {
			if !r.Timestamp.Equal(wantTS) {
				t.Fatalf("batches[%d].Rows[%d].Timestamp = %v, want %v",
					i, j, r.Timestamp, wantTS)
			}
		}
	}
}
