package emit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type noopHost struct{}

func (noopHost) Fatal(string, error) {}

// startEmitter calls e.Start (which is non-blocking) and registers a
// t.Cleanup that calls Shutdown with a 1s grace. The Shutdown error is
// asserted there so cleanup failures fail the test.
func startEmitter(t *testing.T, e *Emitter) {
	t.Helper()
	require.NoError(t, e.Start(t.Context(), noopHost{}), "emitter Start")
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		require.NoError(t, e.Shutdown(stopCtx), "emitter Shutdown")
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
	require.NoError(t, err, "newMetrics")

	writer := &fakeWriter{callCh: make(chan struct{}, 1)}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	require.NoError(t, e.Emit(t.Context(), sampleRows(2)), "Emit")

	waitForCalls(t, writer.callCh, 1)

	require.Equal(t, 1, writer.batchCount(), "batch count")

	batch := writer.lastBatch()
	require.Equal(t, "trace_root_topology", batch.Table)
	require.Len(t, batch.Rows, 2)
	require.WithinDuration(t, fixedNow()(), batch.Rows[0].Timestamp, 0)
}

func TestEmitterFlushesOnTimer(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	cfg := testEmitterConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = config.Duration(5 * time.Millisecond)

	writer := &fakeWriter{callCh: make(chan struct{}, 1)}
	e := newWithWriter(cfg, zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	require.NoError(t, e.Emit(t.Context(), sampleRows(1)), "Emit")

	waitForCalls(t, writer.callCh, 1)

	require.Equal(t, 1, writer.batchCount(), "batch count")
}

func TestEmitterRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	writer := &fakeWriter{
		callCh: make(chan struct{}, 3),
		errs:   []error{errors.New("temporary"), errors.New("temporary")},
	}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	require.NoError(t, e.Emit(t.Context(), sampleRows(2)), "Emit")

	waitForCalls(t, writer.callCh, 3)

	require.Equal(t, 3, writer.batchCount(), "write attempts")
	require.InDelta(t, 0, testutil.ToFloat64(m.emissionsFailed), 0, "dropped rows")
}

func TestEmitterReturnsErrQueueFullAndCountsDrops(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	cfg := testEmitterConfig()
	cfg.QueueCapacity = 1
	cfg.BatchSize = 10
	cfg.FlushInterval = config.Duration(time.Hour)

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	writer := &fakeWriter{}
	e := newWithWriter(cfg, logger, m, writer, fixedNow())
	// Intentionally do NOT start the worker so the queue does not drain.

	require.NoError(t, e.Emit(t.Context(), sampleRows(1)), "first Emit")

	err = e.Emit(t.Context(), sampleRows(2))
	require.ErrorIs(t, err, ErrQueueFull)
	require.InDelta(t, 2, testutil.ToFloat64(m.emissionsFailed), 0, "dropped rows")
	require.NotEmpty(t, logs.All(), "expected warning log for dropped rows")
}

func TestEmitter_ShutdownWithoutStartClosesWriter(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	w := &fakeWriter{}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, w, fixedNow())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, e.Shutdown(ctx), "Shutdown without Start")
	require.True(t, w.closed, "writer.Close not called when Shutdown is invoked without prior Start")
	require.NoError(t, e.Shutdown(ctx), "second Shutdown must be idempotent")
	assert.Equal(t, 1, w.closeCount, "writer.Close call count")
}

func TestEmitterShutdownFlushesPendingRows(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	cfg := testEmitterConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = config.Duration(time.Hour)

	writer := &fakeWriter{}
	e := newWithWriter(cfg, zap.NewNop(), m, writer, fixedNow())
	startEmitter(t, e)

	require.NoError(t, e.Emit(t.Context(), sampleRows(1)), "Emit")

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, e.Shutdown(stopCtx), "Shutdown")

	require.Equal(t, 1, writer.batchCount(), "batch count on stop")
	require.True(t, writer.closed, "writer was not closed")
}

func TestEmitterEmitAfterShutdownReturnsErrClosed(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, &fakeWriter{}, fixedNow())
	startEmitter(t, e)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, e.Shutdown(stopCtx), "Shutdown")

	err = e.Emit(t.Context(), sampleRows(1))
	require.ErrorIs(t, err, ErrClosed)
}

func TestEmitterShutdownReturnsWriteError(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

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

	require.NoError(t, e.Emit(t.Context(), sampleRows(2)), "Emit")

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = e.Shutdown(stopCtx)
	require.ErrorContains(t, err, "write failed", "expected Shutdown to return write error from final flush")
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
	require.Equal(t, "trace_root_topology", batch.Table)
	require.Len(t, batch.Rows, 1)

	got := batch.Rows[0]
	require.WithinDuration(t, ts, got.Timestamp, 0)
	require.Equal(t, rows[0].RootID, got.RootID, "root_id")
	require.Equal(t, rows[0].TraceID, got.TraceID, "trace_id")
	require.Equal(t, rows[0].RootService, got.RootService, "root_service")
	require.Equal(t, rows[0].RootOperation, got.RootOperation, "root_operation")
	require.Equal(t, rows[0].Breadth, got.Breadth, "breadth")
	require.Equal(t, rows[0].ServiceHopDepth, got.ServiceHopDepth, "service_hop_depth")
	require.Equal(t, rows[0].ServiceCount, got.ServiceCount, "service_count")
	require.Equal(t, rows[0].OperationCount, got.OperationCount, "operation_count")
	require.Equal(t, rows[0].SpanCount, got.SpanCount, "span_count")
	require.Equal(t, rows[0].ErrorCount, got.ErrorCount, "error_count")
	require.Equal(t, rows[0].RootDurationNS, got.RootDurationNS, "root_duration_ns")
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
			require.FailNow(t, "timed out waiting for write call")
		}
	}
}

func TestEmitter_EmitAfterShutdownBumpsEmissionsFailed(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, &fakeWriter{}, fixedNow())
	startEmitter(t, e)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, e.Shutdown(stopCtx), "Shutdown")

	rows := sampleRows(3)
	require.ErrorIs(t, e.Emit(context.Background(), rows), ErrClosed, "Emit")

	require.InDelta(t, float64(len(rows)), testutil.ToFloat64(m.emissionsFailed), 0, "topology_emissions_failed_total")
}

func TestEmitter_ShutdownRejectsConcurrentEmitWithoutSilentLoss(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

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
		require.FailNowf(t, "Shutdown should have blocked on Lock", "returned %v while RLock was held", err)
	case <-time.After(50 * time.Millisecond):
	}

	e.queue <- sampleRows(1)
	e.closedMu.RUnlock()

	require.NoError(t, <-stopErr, "Shutdown")
	require.Equal(t, 1, w.batchCount(), "row queued before closed=true must reach the writer")

	require.ErrorIs(t, e.Emit(context.Background(), sampleRows(1)), ErrClosed, "post-Shutdown Emit")
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
	require.NoError(t, err, "newMetrics")

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

	require.NoError(t, e.Start(t.Context(), noopHost{}), "Start")

	require.NoError(t, e.Emit(t.Context(), sampleRows(20)), "Emit")

	time.Sleep(50 * time.Millisecond)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, e.Shutdown(stopCtx), "Shutdown")
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
	require.NoError(t, err, "newMetrics")

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
	require.NoError(t, e.Start(t.Context(), noopHost{}), "Start")

	require.NoError(t, e.Emit(context.Background(), sampleRows(1)), "Emit")

	select {
	case <-w.entered:
	case <-time.After(time.Second):
		require.FailNow(t, "writer.Write was never called; cannot exercise stuck-write path")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()
	stopErr := e.Shutdown(stopCtx)

	require.Error(t, stopErr, "Shutdown returned nil when ctx expired mid-write; want ctx error")
	require.True(t, errors.Is(stopErr, context.DeadlineExceeded) || errors.Is(stopErr, context.Canceled),
		"Shutdown error = %v; want context.DeadlineExceeded or context.Canceled", stopErr)
	require.True(t, w.closed.Load(), "writer.Close() was not called: Shutdown returned with ctx expired and leaked the writer")
	require.Equal(t, int32(1), w.writeCalls.Load(), "writer.Write call count (no extra final flush with already-canceled ctx)")
}

func TestEmitter_ShutdownGraceExpiryRecordsQueuedRowsAsDropped(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

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
	require.NoError(t, e.Start(t.Context(), noopHost{}), "Start")

	require.NoError(t, e.Emit(context.Background(), sampleRows(1)), "Emit (in-flight)")

	select {
	case <-w.entered:
	case <-time.After(time.Second):
		require.FailNow(t, "writer.Write was never called; cannot exercise stuck-write path")
	}

	const queuedRows = 4
	for range queuedRows {
		require.NoError(t, e.Emit(context.Background(), sampleRows(1)), "Emit (queued)")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()
	stopErr := e.Shutdown(stopCtx)
	require.Error(t, stopErr, "Shutdown returned nil when ctx expired; want ctx error")
	require.True(t, errors.Is(stopErr, context.DeadlineExceeded) || errors.Is(stopErr, context.Canceled),
		"Shutdown error = %v; want context.DeadlineExceeded or context.Canceled", stopErr)

	wantDropped := float64(1 + queuedRows)
	require.InDelta(t, wantDropped, testutil.ToFloat64(m.emissionsFailed), 0,
		"topology_emissions_failed_total (1 in-flight + %d queued)", queuedRows)
}

func TestEmitter_ShutdownWithPreCancelledCtxReturnsCtxErr(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

	w := &fakeWriter{}
	e := newWithWriter(testEmitterConfig(), zap.NewNop(), m, w, fixedNow())
	require.NoError(t, e.Start(t.Context(), noopHost{}), "Start")

	stopCtx, stopCancel := context.WithCancel(context.Background())
	stopCancel()

	stopErr := e.Shutdown(stopCtx)
	require.ErrorIs(t, stopErr, context.Canceled, "Shutdown error")
	require.True(t, w.closed, "writer.Close was not called when Shutdown ran with pre-cancelled ctx")
}

func TestEmitter_ShutdownAfterGraceExpiryIsIdempotent(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

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
	require.NoError(t, e.Start(t.Context(), noopHost{}), "Start")

	require.NoError(t, e.Emit(context.Background(), sampleRows(1)), "Emit")
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		require.FailNow(t, "writer.Write was never called")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stopCancel()
	require.Error(t, e.Shutdown(stopCtx), "first Shutdown returned nil; want ctx error")

	dropsAfterFirst := testutil.ToFloat64(m.emissionsFailed)
	writesAfterFirst := w.writeCalls.Load()
	closesAfterFirst := w.closeCalls.Load()
	require.Equal(t, int32(1), closesAfterFirst, "writer.Close calls after first Shutdown")

	require.NoError(t, e.Shutdown(stopCtx), "second Shutdown; want nil (idempotent)")
	require.InDelta(t, dropsAfterFirst, testutil.ToFloat64(m.emissionsFailed), 0,
		"emissionsFailed after second Shutdown (no double-count)")
	require.Equal(t, writesAfterFirst, w.writeCalls.Load(), "writer.Write calls after second Shutdown")
	require.Equal(t, closesAfterFirst, w.closeCalls.Load(), "writer.Close calls after second Shutdown (close exactly once)")
}

func TestEmitter_RetryReusesBatchTimestamp(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	require.NoError(t, err, "newMetrics")

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

	require.NoError(t, e.Emit(context.Background(), sampleRows(2)), "Emit")

	waitForCalls(t, writer.callCh, 3)

	require.Equal(t, 3, writer.batchCount(), "write attempts (2 failed + 1 success)")

	writer.mu.Lock()
	batches := append([]writeBatch(nil), writer.batches...)
	writer.mu.Unlock()

	wantTS := batches[0].Rows[0].Timestamp
	for i, b := range batches {
		for j, r := range b.Rows {
			require.WithinDuration(t, wantTS, r.Timestamp, 0, "batches[%d].Rows[%d].Timestamp", i, j)
		}
	}
}
