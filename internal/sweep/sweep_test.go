package sweep

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/emit"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

type fakeBuffer struct {
	scanResult []buffer.Finalizable
	scanErr    error
	drainSpans map[[16]byte]map[string]buffer.SpanRecord
	drainErrs  map[[16]byte]error
	onScan     func()

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
	onScan := f.onScan
	f.mu.Unlock()
	if onScan != nil {
		onScan()
	}
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
	rm      topology.RootMetrics
	orphans int32
	err     error
}

func (c *fakeComputer) Compute(id [16]byte, _ string, _ map[string]buffer.SpanRecord) (topology.RootMetrics, int32, error) {
	c.calls.Add(1)
	r := c.results[id]
	return r.rm, r.orphans, r.err
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

func (*fakeEmitter) Close(context.Context) error { return nil }

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
func newSweeperForTest(t *testing.T, buf Buffer, c Computer, e emit.Emitter) (*Sweeper, *Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	s, err := New(buf, c, e, Config{
		QuietPeriod:    30 * time.Second,
		MaxTTL:         5 * time.Minute,
		Interval:       10 * time.Millisecond,
		WorkerPoolSize: 4,
	}, m, zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	return s, m
}

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

	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 1 {
		t.Fatalf("sweeps_total{ok}: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultScanError)); got != 0 {
		t.Fatalf("sweeps_total{scan_error}: want 0, got %v", got)
	}
	if len(buf.drainCalls) != 0 {
		t.Fatalf("no Drain calls expected, got %d", len(buf.drainCalls))
	}
	if got := comp.calls.Load(); got != 0 {
		t.Fatalf("no Compute calls expected, got %d", got)
	}
	if em.callCount() != 0 {
		t.Fatalf("no Emit calls expected, got %d", em.callCount())
	}
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
			a: {rm: topology.RootMetrics{TraceID: "a"}},
			b: {rm: topology.RootMetrics{TraceID: "b"}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	if got := testutil.ToFloat64(m.finalized); got != 2 {
		t.Fatalf("finalized: want 2, got %v", got)
	}
	if got := testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerQuiet)); got != 1 {
		t.Fatalf("trigger{quiet}: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerMaxTTL)); got != 1 {
		t.Fatalf("trigger{max_ttl}: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 1 {
		t.Fatalf("sweeps{ok}: want 1, got %v", got)
	}
	if em.callCount() != 1 {
		t.Fatalf("Emit calls: want 1, got %d", em.callCount())
	}
	if rows := em.rowsIn(0); len(rows) != 2 {
		t.Fatalf("emitted rows: want 2, got %d", len(rows))
	}
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
			b: {rm: topology.RootMetrics{TraceID: "b"}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	if got := testutil.ToFloat64(m.drainErrors); got != 1 {
		t.Fatalf("drain_errors: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerQuiet)); got != 1 {
		t.Fatalf("trigger{quiet}: want 1, got %v", got)
	}
	if em.callCount() != 1 {
		t.Fatalf("Emit calls: want 1, got %d", em.callCount())
	}
	if rows := em.rowsIn(0); len(rows) != 1 || rows[0].TraceID != "b" {
		t.Fatalf("emitted rows: want [{b}], got %+v", rows)
	}
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

	if got := testutil.ToFloat64(m.computeSkipped.WithLabelValues(reasonNoRoot)); got != 1 {
		t.Fatalf("compute_skipped{no_root}: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.computeErrors); got != 0 {
		t.Fatalf("compute_errors: want 0, got %v", got)
	}
	if got := testutil.ToFloat64(m.orphanSpans); got != 3 {
		t.Fatalf("orphan_spans: want 3, got %v", got)
	}
	if got := histogramCount(t, m.computeDuration); got != 1 {
		t.Fatalf("compute_duration observations: want 1, got %d", got)
	}
	if got := testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerMaxTTL)); got != 0 {
		t.Fatalf("trigger{max_ttl}: want 0, got %v", got)
	}
	if em.callCount() != 0 {
		t.Fatalf("Emit calls: want 0, got %d", em.callCount())
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 1 {
		t.Fatalf("sweeps{ok}: want 1, got %v", got)
	}
}

func histogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var mt dto.Metric
	if err := h.Write(&mt); err != nil {
		t.Fatalf("histogram Write: %v", err)
	}
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
			b: {rm: topology.RootMetrics{TraceID: "b"}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	if got := testutil.ToFloat64(m.computeErrors); got != 1 {
		t.Fatalf("compute_errors: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 1 {
		t.Fatalf("sweeps{ok}: want 1, got %v", got)
	}
	if em.callCount() != 1 {
		t.Fatalf("Emit calls: want 1, got %d", em.callCount())
	}
	if rows := em.rowsIn(0); len(rows) != 1 || rows[0].TraceID != "b" {
		t.Fatalf("emitted rows: want [{b}], got %+v", rows)
	}
}

func TestSweeper_ScanError_StopsTick(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{scanErr: errors.New("valkey down")}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultScanError)); got != 1 {
		t.Fatalf("sweeps{scan_error}: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 0 {
		t.Fatalf("sweeps{ok}: want 0, got %v", got)
	}
	if len(buf.drainCalls) != 0 {
		t.Fatalf("no Drain calls expected, got %d", len(buf.drainCalls))
	}
	if em.callCount() != 0 {
		t.Fatalf("no Emit calls expected, got %d", em.callCount())
	}
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
			a: {rm: topology.RootMetrics{TraceID: "a"}},
		},
	}
	em := &fakeEmitter{err: emit.ErrQueueFull}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultEmitError)); got != 1 {
		t.Fatalf("sweeps{emit_error}: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 0 {
		t.Fatalf("sweeps{ok}: want 0, got %v", got)
	}
	if got := testutil.ToFloat64(m.trigger.WithLabelValues(buffer.TriggerQuiet)); got != 1 {
		t.Fatalf("trigger{quiet}: want 1, got %v", got)
	}
}

func TestSweeper_TickComputesCutoffsFromNow(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, _ := newSweeperForTest(t, buf, comp, em)

	s.tick(context.Background())

	if buf.scanCalls != 1 {
		t.Fatalf("Scan calls: want 1, got %d", buf.scanCalls)
	}
	now := s.now()
	wantQuiet := now.Add(-s.cfg.QuietPeriod)
	wantTTL := now.Add(-s.cfg.MaxTTL)
	if got := buf.scanCutoffs[0]; !got.quiet.Equal(wantQuiet) || !got.ttl.Equal(wantTTL) {
		t.Fatalf("cutoffs: want quiet=%v ttl=%v, got quiet=%v ttl=%v",
			wantQuiet, wantTTL, got.quiet, got.ttl)
	}
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
	}, m, zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan struct{})
	go func() {
		s.tick(context.Background())
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("tick completed before all workers arrived at the gate — pool is not concurrent")
	case <-time.After(50 * time.Millisecond):
	}
	if got := arrivals.Load(); got != pool {
		t.Fatalf("arrivals at gate: want %d, got %d", pool, got)
	}
	close(gate)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("tick did not complete after gate release")
	}
	if em.callCount() != 1 {
		t.Fatalf("Emit calls: want 1, got %d", em.callCount())
	}
	if rows := em.rowsIn(0); len(rows) != pool {
		t.Fatalf("emitted rows: want %d, got %d", pool, len(rows))
	}
}

type gatingComputer struct {
	gate    chan struct{}
	arrived *atomic.Int32
	target  int32
}

func (g *gatingComputer) Compute(_ [16]byte, _ string, _ map[string]buffer.SpanRecord) (topology.RootMetrics, int32, error) {
	if g.arrived.Add(1) > g.target {
		return topology.RootMetrics{}, 0, errors.New("more arrivals than pool size")
	}
	<-g.gate
	return topology.RootMetrics{}, 0, nil
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

	if len(buf.drainCalls) != 0 {
		t.Fatalf("no Drain calls expected, got %d", len(buf.drainCalls))
	}
	if em.callCount() != 0 {
		t.Fatalf("no Emit calls expected, got %d", em.callCount())
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultScanError)); got != 0 {
		t.Fatalf("sweeps{scan_error}: want 0, got %v", got)
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 0 {
		t.Fatalf("sweeps{ok}: want 0, got %v", got)
	}
}

// TestSweeper_Tick_CancelAfterScan_CompletesDrainEmit: once Scan returns
// finalizables, Drain's delete must still reach Emit even if parent cancels.
func TestSweeper_Tick_CancelAfterScan_CompletesDrainEmit(t *testing.T) {
	t.Parallel()

	a := traceID(1)
	ctx, cancel := context.WithCancel(context.Background())
	buf := &fakeBuffer{
		scanResult: []buffer.Finalizable{{TraceID: a, Trigger: buffer.TriggerQuiet}},
		drainSpans: map[[16]byte]map[string]buffer.SpanRecord{
			a: {"span-a": {TraceID: a}},
		},
		onScan: cancel,
	}
	comp := &fakeComputer{
		results: map[[16]byte]computeResult{
			a: {rm: topology.RootMetrics{TraceID: "a"}},
		},
	}
	em := &fakeEmitter{}
	s, m := newSweeperForTest(t, buf, comp, em)

	s.tick(ctx)

	if buf.scanCalls != 1 {
		t.Fatalf("Scan calls: want 1, got %d", buf.scanCalls)
	}
	if len(buf.drainCalls) != 1 {
		t.Fatalf("Drain calls: want 1, got %d", len(buf.drainCalls))
	}
	if em.callCount() != 1 {
		t.Fatalf("Emit calls: want 1, got %d", em.callCount())
	}
	if got := testutil.ToFloat64(m.finalized); got != 1 {
		t.Fatalf("finalized: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.sweeps.WithLabelValues(resultOK)); got != 1 {
		t.Fatalf("sweeps{ok}: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.drainErrors); got != 0 {
		t.Fatalf("drain_errors: want 0, got %v", got)
	}
}

func TestSweeper_Run_ReturnsOnCtxCancel(t *testing.T) {
	t.Parallel()

	buf := &fakeBuffer{}
	comp := &fakeComputer{}
	em := &fakeEmitter{}
	s, _ := newSweeperForTest(t, buf, comp, em)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: want nil, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestNewMetrics_NilRegisterer(t *testing.T) {
	t.Parallel()

	m := NewMetrics(nil)
	if m != nil {
		t.Fatalf("NewMetrics(nil): want nil, got %+v", m)
	}
	m.incSweep(resultOK)
	m.incFinalized(buffer.TriggerQuiet)
	m.incDrainError()
	m.incComputeError()
	m.incComputeSkipped(reasonNoRoot)
	m.addOrphanSpans(3)
	m.observeComputeDuration(time.Millisecond)
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
		e    emit.Emitter
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
			if _, err := New(tc.buf, tc.c, tc.e, tc.cfg, m, tc.log); err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
}
