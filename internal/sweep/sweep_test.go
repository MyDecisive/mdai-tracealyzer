package sweep

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/emit"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeBuffer struct {
	scanResult []buffer.Finalizable
	scanErr    error
	drainSpans map[[16]byte]map[string]buffer.SpanRecord
	drainErrs  map[[16]byte]error

	mu          sync.Mutex
	scanCalls   int
	drainCalls  [][16]byte
	scanCutoffs []scanArgs
}

type scanArgs struct {
	quiet time.Time
	ttl   time.Time
}

func (f *fakeBuffer) Scan(ctx context.Context, quietCutoff, ttlCutoff time.Time) ([]buffer.Finalizable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.scanCalls++
	f.scanCutoffs = append(f.scanCutoffs, scanArgs{quietCutoff, ttlCutoff})
	f.mu.Unlock()
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	return f.scanResult, nil
}

func (f *fakeBuffer) Drain(ctx context.Context, id [16]byte) (map[string]buffer.SpanRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.drainCalls = append(f.drainCalls, id)
	f.mu.Unlock()
	if err, ok := f.drainErrs[id]; ok {
		return nil, err
	}
	return f.drainSpans[id], nil
}

type fakeComputer struct {
	calls   atomic.Int64
	results map[[16]byte]computeResult
}

type computeResult struct {
	rows    []topology.RootMetrics
	orphans int32
	err     error
}

func (c *fakeComputer) Compute(id [16]byte, _ map[string]buffer.SpanRecord) ([]topology.RootMetrics, int32, error) {
	c.calls.Add(1)
	r := c.results[id]
	return r.rows, r.orphans, r.err
}

type fakeEmitter struct {
	mu    sync.Mutex
	calls [][]topology.RootMetrics
	err   error
}

func (e *fakeEmitter) Emit(_ context.Context, rows []topology.RootMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	cloned := make([]topology.RootMetrics, len(rows))
	copy(cloned, rows)
	e.calls = append(e.calls, cloned)
	return e.err
}

func (e *fakeEmitter) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func (e *fakeEmitter) rowsIn(call int) []topology.RootMetrics {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[call]
}

// newSweeperForTest builds a Sweeper with a fixed wall clock, short
// interval (unused when tests call tick directly), and a nop logger.
func newSweeperForTest(t *testing.T, buf Buffer, c Computer, e Emitter) (*Sweeper, *Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	s, err := New(buf, c, e, Config{
		QuietPeriod:    30 * time.Second,
		MaxTTL:         5 * time.Minute,
		Interval:       10 * time.Millisecond,
		WorkerPoolSize: 4,
	}, nil, m, zap.NewNop())
	require.NoError(t, err, "New")
	s.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	return s, m
}

type noopHost struct{}

func (noopHost) Fatal(string, error) {}

func traceID(b byte) [16]byte {
	var id [16]byte
	id[15] = b
	return id
}

func TestSweeper_EmptyScan_MarksOK(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)), 0, "sweeps_total{ok}")
	require.InDelta(t, 0, testutil.ToFloat64(m.sweeps.WithLabelValues(resultScanError)), 0, "sweeps_total{scan_error}")
	require.Empty(t, buf.drainCalls, "no Drain calls expected")
	require.Zero(t, comp.calls.Load(), "no Compute calls expected")
	require.Zero(t, em.callCount(), "no Emit calls expected")
}

func TestSweeper_MultiRootTrace_EmitsOneRowPerRoot(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerQuiet}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {rows: []topology.RootMetrics{
				{TraceID: "a", RootService: "svc-a", RootOperation: "root-a"},
				{TraceID: "a", RootService: "svc-b", RootOperation: "root-b"},
			}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.finalized), 0, "finalized (per trace)")
	require.InDelta(t, 1, testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerQuiet)), 0, "trigger{quiet}")
	require.Equal(t, 1, em.callCount(), "Emit calls")
	rows := em.rowsIn(0)
	require.Len(t, rows, 2, "emitted rows")
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.RootService+"::"+r.RootOperation] = true
	}
	require.True(t, seen["svc-a::root-a"] && seen["svc-b::root-b"], "expected both roots in emitted batch, got %+v", rows)
}

func TestSweeper_TwoFinalizable_BothEmittedInOneBatch(t *testing.T) {
	t.Parallel()

	a, b := traceID(1), traceID(2)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{
			{TraceID: a, Trigger: buffer.TriggerQuiet},
			{TraceID: b, Trigger: buffer.TriggerMaxTTL},
		},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
			b: {"span-b": {TraceID: b}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {rows: []topology.RootMetrics{{TraceID: "a"}}},
			b: {rows: []topology.RootMetrics{{TraceID: "b"}}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 2, testutil.ToFloat64(m.finalized), 0, "finalized")
	require.InDelta(t, 1, testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerQuiet)), 0, "trigger{quiet}")
	require.InDelta(t, 1, testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerMaxTTL)), 0, "trigger{max_ttl}")
	require.InDelta(t, 1, testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)), 0, "sweeps{ok}")
	require.Equal(t, 1, em.callCount(), "Emit calls")
	require.Len(t, em.rowsIn(0), 2, "emitted rows")
}

func TestSweeper_DrainErrorIsolatesFailure(t *testing.T) {
	t.Parallel()

	a, b := traceID(1), traceID(2)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{
			{TraceID: a, Trigger: buffer.TriggerQuiet},
			{TraceID: b, Trigger: buffer.TriggerQuiet},
		},
		drainErrs: map[[16]byte]error{
			a: errors.New("backend down"),
		},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			b: {"span-b": {TraceID: b}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			b: {rows: []topology.RootMetrics{{TraceID: "b"}}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.drainErrors), 0, "drain_errors")
	require.InDelta(t, 1, testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerQuiet)), 0, "trigger{quiet}")
	require.Equal(t, 1, em.callCount(), "Emit calls")
	rows := em.rowsIn(0)
	require.Len(t, rows, 1, "emitted rows")
	require.Equal(t, "b", rows[0].TraceID, "emitted rows")
}

func TestSweeper_NoRoot_IsExpectedSkip(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerMaxTTL}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {orphans: 3, err: ErrNoRoot},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.computeSkipped.WithLabelValues(reasonNoRoot)), 0, "compute_skipped{no_root}")
	require.InDelta(t, 0, testutil.ToFloat64(m.computeErrors), 0, "compute_errors")
	require.InDelta(t, 3, testutil.ToFloat64(m.orphanSpans), 0, "orphan_spans")
	require.Equal(t, uint64(1), histogramCount(t, m.computeDuration), "compute_duration observations")
	require.InDelta(t, 0, testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerMaxTTL)), 0, "trigger{max_ttl}")
	require.Zero(t, em.callCount(), "Emit calls")
	require.InDelta(t, 1, testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)), 0, "sweeps{ok}")
}

func histogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var mt dto.Metric
	require.NoError(t, h.Write(&mt), "histogram Write")
	return mt.GetHistogram().GetSampleCount()
}

func TestSweeper_ComputeError_DropsRowTickOK(t *testing.T) {
	t.Parallel()

	a, b := traceID(1), traceID(2)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{
			{TraceID: a, Trigger: buffer.TriggerQuiet},
			{TraceID: b, Trigger: buffer.TriggerQuiet},
		},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
			b: {"span-b": {TraceID: b}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {err: errors.New("compute bug")},
			b: {rows: []topology.RootMetrics{{TraceID: "b"}}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.computeErrors), 0, "compute_errors")
	require.InDelta(t, 1, testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)), 0, "sweeps{ok}")
	require.Equal(t, 1, em.callCount(), "Emit calls")
	rows := em.rowsIn(0)
	require.Len(t, rows, 1, "emitted rows")
	require.Equal(t, "b", rows[0].TraceID, "emitted rows")
}

func TestSweeper_ScanError_StopsTick(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{scanErr: errors.New("valkey down")}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.sweeps.WithLabelValues(resultScanError)), 0, "sweeps{scan_error}")
	require.InDelta(t, 0, testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)), 0, "sweeps{ok}")
	require.Empty(t, buf.drainCalls, "no Drain calls expected")
	require.Zero(t, em.callCount(), "no Emit calls expected")
}

func TestSweeper_EmitError_MarksEmitError(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerQuiet}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {rows: []topology.RootMetrics{{TraceID: "a"}}},
		},
	}
	em := &fakeEmitter{err: emit.ErrQueueFull}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.sweeps.WithLabelValues(resultEmitError)), 0, "sweeps{emit_error}")
	require.InDelta(t, 0, testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)), 0, "sweeps{ok}")
	require.InDelta(t, 1, testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerQuiet)), 0, "trigger{quiet}")
}

func TestSweeper_TickComputesCutoffsFromNow(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, _ := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.Equal(t, 1, buf.scanCalls, "Scan calls")
	now := s.now()
	wantQuiet := now.Add(-s.cfg.QuietPeriod)
	wantTTL := now.Add(-s.cfg.MaxTTL)
	got := buf.scanCutoffs[0]
	require.WithinDuration(t, wantQuiet, got.quiet, 0, "quiet cutoff")
	require.WithinDuration(t, wantTTL, got.ttl, 0, "ttl cutoff")
}

// TestSweeper_FanoutRunsConcurrently verifies the worker pool drives
// multiple Compute calls in parallel. The gating computer blocks each call
// until pool-size calls have arrived; if the sweep were serial, the test
// would deadlock and trip the 2s timeout.
func TestSweeper_FanoutRunsConcurrently(t *testing.T) {
	t.Parallel()

	const pool = 4
	ids := make([]buffer.Finalizable, pool)
	drain := make(map[[16]byte]map[string]buffer.SpanRecord, pool)
	for i := range pool {
		id := traceID(byte(i + 1))
		ids[i] = buffer.Finalizable{TraceID: id, Trigger: buffer.TriggerQuiet}
		drain[id] = map[string]buffer.SpanRecord{"s": {TraceID: id}}
	}
	buf := &fakeBuffer{scanResult: ids, drainSpans: drain}

	gate := make(chan struct{})
	var arrivals atomic.Int32
	comp := &gatingComputer{
		gate:    gate,
		arrived: &arrivals,
		target:  pool,
	}
	em := &fakeEmitter{}
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	s, err := New(buf, comp, em, Config{
		QuietPeriod:    30 * time.Second,
		MaxTTL:         5 * time.Minute,
		Interval:       10 * time.Millisecond,
		WorkerPoolSize: pool,
	}, nil, m, zap.NewNop())
	require.NoError(t, err, "New")

	done := make(chan struct{})
	go func() {
		s.tick(context.Background())
		close(done)
	}()

	select {
	case <-done:
		require.Fail(t, "tick completed before all workers arrived at the gate — pool is not concurrent")
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, int32(pool), arrivals.Load(), "arrivals at gate")
	close(gate)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "tick did not complete after gate release")
	}
	require.Equal(t, 1, em.callCount(), "Emit calls")
	require.Len(t, em.rowsIn(0), pool, "emitted rows")
}

type gatingComputer struct {
	gate    chan struct{}
	arrived *atomic.Int32
	target  int32
}

func (g *gatingComputer) Compute(_ [16]byte, _ map[string]buffer.SpanRecord) ([]topology.RootMetrics, int32, error) {
	if g.arrived.Add(1) > g.target {
		return nil, 0, errors.New("more arrivals than pool size")
	}
	<-g.gate
	return []topology.RootMetrics{{}}, 0, nil
}

func TestSweeper_Tick_CancelBeforeScan_IsBenignSkip(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.tick(ctx)

	require.Empty(t, buf.drainCalls, "no Drain calls expected")
	require.Zero(t, em.callCount(), "no Emit calls expected")
	require.InDelta(t, 0, testutil.ToFloat64(m.sweeps.WithLabelValues(resultScanError)), 0, "sweeps{scan_error}")
	require.InDelta(t, 0, testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)), 0, "sweeps{ok}")
}

func TestSweeper_StartIsNonBlockingShutdownHalts(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, _ := newSweeperForTest(t, buf, comp, em)

	require.NoError(t, s.Start(t.Context(), noopHost{}), "Start")
	require.NoError(t, s.Shutdown(context.Background()), "Shutdown")
}

func TestSweeper_DoesNotDrainBeforeReadinessGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := traceID(1)
		buf := &fakeBuffer{
			scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerQuiet}},
			drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
				a: {"span-a": {TraceID: a}},
			},
		}
		comp := &fakeComputer{}
		em := &fakeEmitter{}

		gate := make(chan struct{})
		reg := prometheus.NewRegistry()
		s, err := New(buf, comp, em, Config{
			QuietPeriod:    30 * time.Second,
			MaxTTL:         5 * time.Minute,
			Interval:       time.Millisecond,
			WorkerPoolSize: 1,
		}, gate, NewMetrics(reg), zap.NewNop())
		require.NoError(t, err, "New")
		s.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

		require.NoError(t, s.Start(t.Context(), noopHost{}), "Start")
		// Advance the fake clock past several intervals; the ticker only fires
		// once run() has passed the gate, so nothing must be scanned.
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assert.Zero(t, buf.scanCalls, "Scan must not run before gate fires")
		assert.Empty(t, buf.drainCalls, "Drain must not run before gate fires")
		require.NoError(t, s.Shutdown(context.Background()), "Shutdown")
	})
}

func TestSweeper_ClaimedTraceCompletesAcrossShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := traceID(1)
		drainEntered := make(chan struct{})
		releaseDrain := make(chan struct{})
		buf := &blockingBuffer{
			traceID:      a,
			drainEntered: drainEntered,
			releaseDrain: releaseDrain,
		}
		comp := &fakeComputer{
			results: map[[16]byte]computeResult{
				a: {rows: []topology.RootMetrics{{TraceID: "a"}}},
			},
		}
		em := &fakeEmitter{}

		gate := make(chan struct{})
		close(gate)
		reg := prometheus.NewRegistry()
		s, err := New(buf, comp, em, Config{
			QuietPeriod:    30 * time.Second,
			MaxTTL:         5 * time.Minute,
			Interval:       time.Millisecond,
			WorkerPoolSize: 1,
		}, gate, NewMetrics(reg), zap.NewNop())
		require.NoError(t, err, "New")
		s.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

		require.NoError(t, s.Start(t.Context(), noopHost{}), "Start")

		// Deadlines are fake-time; the ticker keeps advancing the clock, so a
		// stuck sweeper fails here instead of spinning until the test timeout.
		select {
		case <-drainEntered: // clock auto-advances to the first tick; Drain blocks
		case <-time.After(time.Second):
			require.Fail(t, "Drain never entered")
		}

		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- s.Shutdown(context.Background()) }()

		// Shutdown must wait on the in-flight Drain.
		synctest.Wait()
		select {
		case err := <-shutdownDone:
			require.Fail(t, "Shutdown returned while Drain was still in flight", "err: %v", err)
		default:
		}

		close(releaseDrain)

		select {
		case err := <-shutdownDone:
			require.NoError(t, err, "Shutdown")
		case <-time.After(time.Second):
			require.Fail(t, "Shutdown did not return after Drain released")
		}
		require.Equal(t, 1, em.callCount(), "Emit calls (the claimed trace's row)")
	})
}

func TestSweeper_UnclaimedTracesLeftInValkeyAcrossShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := traceID(1)
		drainEntered := make(chan struct{})
		releaseDrain := make(chan struct{})
		buf := &blockingBuffer{
			traceID:      a,
			extra:        []buffer.Finalizable{{TraceID: traceID(2), Trigger: buffer.TriggerQuiet}, {TraceID: traceID(3), Trigger: buffer.TriggerQuiet}},
			drainEntered: drainEntered,
			releaseDrain: releaseDrain,
		}
		comp := &fakeComputer{
			results: map[[16]byte]computeResult{
				a: {rows: []topology.RootMetrics{{TraceID: "a"}}},
			},
		}
		em := &fakeEmitter{}

		gate := make(chan struct{})
		close(gate)
		reg := prometheus.NewRegistry()
		s, err := New(buf, comp, em, Config{
			QuietPeriod:    30 * time.Second,
			MaxTTL:         5 * time.Minute,
			Interval:       time.Millisecond,
			WorkerPoolSize: 1,
		}, gate, NewMetrics(reg), zap.NewNop())
		require.NoError(t, err, "New")
		s.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

		require.NoError(t, s.Start(t.Context(), noopHost{}), "Start")

		select {
		case <-drainEntered:
		case <-time.After(time.Second):
			require.Fail(t, "Drain never entered")
		}

		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- s.Shutdown(context.Background()) }()

		// Wait for the claim loop to observe stopCh (Shutdown has closed it and
		// is blocked on doneCh) before releasing Drain.
		synctest.Wait()
		close(releaseDrain)

		select {
		case err := <-shutdownDone:
			require.NoError(t, err, "Shutdown")
		case <-time.After(time.Second):
			require.Fail(t, "Shutdown did not return")
		}
		assert.Len(t, buf.drainCalls, 1, "only the claimed trace must be drained")
	})
}

// blockingBuffer holds Drain mid-call so tests can observe shutdown
// behavior with a Drain in flight. The first finalizable returned has
// traceID; extra appears after it (used to verify post-stop traces stay
// unclaimed).
type blockingBuffer struct {
	traceID [16]byte
	extra   []buffer.Finalizable

	drainEntered chan struct{}
	releaseDrain chan struct{}
	enterOnce    sync.Once

	mu         sync.Mutex
	drainCalls [][16]byte
}

func (b *blockingBuffer) Scan(_ context.Context, _, _ time.Time) ([]buffer.Finalizable, error) {
	out := []buffer.Finalizable{{TraceID: b.traceID, Trigger: buffer.TriggerQuiet}}
	out = append(out, b.extra...)
	return out, nil
}

func (b *blockingBuffer) Drain(_ context.Context, id [16]byte) (map[string]buffer.SpanRecord, error) {
	b.mu.Lock()
	b.drainCalls = append(b.drainCalls, id)
	b.mu.Unlock()
	b.enterOnce.Do(func() { close(b.drainEntered) })
	<-b.releaseDrain
	return map[string]buffer.SpanRecord{"span": {TraceID: id}}, nil
}

func TestNewMetrics_NilRegisterer(t *testing.T) {
	t.Parallel()

	m := NewMetrics(nil)
	require.Nil(t, m, "NewMetrics(nil)")
	m.incSweep(resultOK)
	m.incFinalized(buffer.TriggerQuiet)
	m.incDrainError()
	m.incComputeError()
	m.incComputeSkipped(reasonNoRoot)
	m.addOrphanSpans(3)
	m.addOrphanBytes(123)
	m.observeComputeDuration(time.Millisecond)
}

func TestOrphanBytes_TotalMinusAttributed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		spans map[string]buffer.SpanRecord
		rows  []topology.RootMetrics
		want  int64
	}{
		{
			name: "all reachable: zero",
			spans: map[string]buffer.SpanRecord{
				"a": {SizeBytes: 100},
				"b": {SizeBytes: 50},
			},
			rows: []topology.RootMetrics{{SpanBytesTotal: 150}},
			want: 0,
		},
		{
			name: "some orphan: total minus attributed",
			spans: map[string]buffer.SpanRecord{
				"a": {SizeBytes: 100},
				"b": {SizeBytes: 50},
				"c": {SizeBytes: 30},
			},
			rows: []topology.RootMetrics{{SpanBytesTotal: 100}},
			want: 80,
		},
		{
			name: "no rows (all orphan): full total",
			spans: map[string]buffer.SpanRecord{
				"a": {SizeBytes: 100},
				"b": {SizeBytes: 50},
			},
			rows: nil,
			want: 150,
		},
		{
			name: "attributed exceeds total: clamped to zero",
			spans: map[string]buffer.SpanRecord{
				"a": {SizeBytes: 10},
			},
			rows: []topology.RootMetrics{{SpanBytesTotal: 999}},
			want: 0,
		},
		{
			name:  "empty inputs",
			spans: nil,
			rows:  nil,
			want:  0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, orphanBytes(tc.spans, tc.rows), "orphanBytes")
		})
	}
}

func TestSweeper_AllOrphan_BytesGoToOrphanMetricNotRow(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerMaxTTL}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {
				"orphan-1": {TraceID: a, SizeBytes: 200},
				"orphan-2": {TraceID: a, SizeBytes: 350},
			},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {orphans: 2, err: ErrNoRoot},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.computeSkipped.WithLabelValues(reasonNoRoot)), 0, "compute_skipped{no_root}")
	require.InDelta(t, 2, testutil.ToFloat64(m.orphanSpans), 0, "orphan_spans")
	require.InDelta(t, 550, testutil.ToFloat64(m.orphanBytes), 0, "orphan_bytes (200+350)")
	require.Zero(t, em.callCount(), "Emit calls on ErrNoRoot")
}

func TestSweeper_PartialOrphan_BytesAccountedAtMetric(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerQuiet}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {
				"reachable-1": {TraceID: a, SizeBytes: 100},
				"reachable-2": {TraceID: a, SizeBytes: 200},
				"orphan-1":    {TraceID: a, SizeBytes: 75},
			},
		},
	}
	// Computer reports a row whose attributed bytes (300) covers two of the
	// three drained spans; the third (75 bytes) is orphan.
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {
				rows:    []topology.RootMetrics{{TraceID: "a", SpanBytesTotal: 300}},
				orphans: 1,
			},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 1, testutil.ToFloat64(m.orphanSpans), 0, "orphan_spans")
	require.InDelta(t, 75, testutil.ToFloat64(m.orphanBytes), 0, "orphan_bytes (375-300)")
	require.Equal(t, 1, em.callCount(), "Emit calls")
	rows := em.rowsIn(0)
	require.Len(t, rows, 1, "emitted rows")
	require.Equal(t, int64(300), rows[0].SpanBytesTotal, "emitted row SpanBytesTotal")
}

func TestNew_ValidatesInputs(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	buf := &fakeBuffer{}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	goodCfg := Config{QuietPeriod: time.Second, MaxTTL: 2 * time.Second, Interval: time.Second, WorkerPoolSize: 1}

	cases := []struct {
		name string
		buf  Buffer
		c    Computer
		e    Emitter
		cfg  Config
		log  *zap.Logger
	}{
		{"nil buffer", nil, comp, em, goodCfg, logger},
		{"nil computer", buf, nil, em, goodCfg, logger},
		{"nil emitter", buf, comp, nil, goodCfg, logger},
		{"nil logger", buf, comp, em, goodCfg, nil},
		{"zero interval", buf, comp, em, Config{QuietPeriod: time.Second, MaxTTL: 2 * time.Second, WorkerPoolSize: 1}, logger},
		{"zero quiet", buf, comp, em, Config{MaxTTL: 2 * time.Second, Interval: time.Second, WorkerPoolSize: 1}, logger},
		{"zero max_ttl", buf, comp, em, Config{QuietPeriod: time.Second, Interval: time.Second, WorkerPoolSize: 1}, logger},
		{"zero worker_pool_size", buf, comp, em, Config{QuietPeriod: time.Second, MaxTTL: 2 * time.Second, Interval: time.Second}, logger},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := NewMetrics(prometheus.NewRegistry())
			_, err := New(tc.buf, tc.c, tc.e, tc.cfg, nil, m, tc.log)
			require.Error(t, err)
		})
	}
}

func TestSweeper_RootIDCollision_CountsMergedRows(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerQuiet}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {rows: []topology.RootMetrics{
				{TraceID: "a", RootID: "svc::POST /orders"},
				{TraceID: "a", RootID: "svc::POST /orders"},
				{TraceID: "a", RootID: "svc::POST /orders"},
				{TraceID: "a", RootID: "svc::GET /health"},
			}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 2, testutil.ToFloat64(m.rootIDCollisions), 0, "root_id_collisions_total (4 rows, 2 distinct RootIDs)")
}

func TestSweeper_NoRootIDCollision_LeavesCounterZero(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerQuiet}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
		},
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {rows: []topology.RootMetrics{
				{TraceID: "a", RootID: "svc::a"},
				{TraceID: "a", RootID: "svc::b"},
			}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	require.InDelta(t, 0, testutil.ToFloat64(m.rootIDCollisions), 0, "root_id_collisions_total")
}
