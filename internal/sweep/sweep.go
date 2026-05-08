package sweep

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"go.uber.org/zap"
)

var _ run.Component = (*Sweeper)(nil)

// Emitter is the narrow contract the sweeper depends on. The full emitter
// in package emit also has Component lifecycle methods, which the sweeper
// does not invoke.
type Emitter interface {
	Emit(ctx context.Context, rows []topology.RootMetrics) error
}

// ErrNoRoot is the sentinel a Computer returns when a trace has no
// discoverable root span. The sweeper treats it as an expected skip,
// not an error. Any other error from Compute is a compute bug and
// increments the error counter.
var ErrNoRoot = errors.New("no root span")

// Computer implementations return ErrNoRoot (directly or wrapped) when
// no root span is discoverable; any other error is treated as a compute bug.
// The int32 return is the orphan-span count for the trace and is recorded
// independently of the error: orphans may be non-zero even when ErrNoRoot
// is returned (e.g. a trace whose every span was unreachable from any root).
// A trace with multiple authentic roots yields one RootMetrics per root.
type Computer interface {
	Compute(traceID [16]byte, spans map[string]buffer.SpanRecord) ([]topology.RootMetrics, int32, error)
}

// Buffer is the subset of buffer.Buffer that the sweeper depends on.
type Buffer interface {
	Scan(ctx context.Context, quietCutoff, ttlCutoff time.Time) ([]buffer.Finalizable, error)
	Drain(ctx context.Context, traceID [16]byte) (map[string]buffer.SpanRecord, error)
}

type Config struct {
	QuietPeriod    time.Duration
	MaxTTL         time.Duration
	Interval       time.Duration
	WorkerPoolSize int
}

// Sweeper drives finalization on a ticker: Scan → Drain → Compute → Emit.
type Sweeper struct {
	buf      Buffer
	computer Computer
	emitter  Emitter
	cfg      Config
	gate     <-chan struct{}
	metrics  *Metrics
	logger   *zap.Logger
	now      func() time.Time

	started   bool
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

// New constructs a Sweeper. gate is awaited inside the spawned goroutine
// before the first tick; pass nil for no gate (tests). In production gate
// is the readiness conjunction (`schema` ∧ `emitter`).
func New(buf Buffer, c Computer, e Emitter, cfg Config, gate <-chan struct{}, m *Metrics, logger *zap.Logger) (*Sweeper, error) {
	if buf == nil {
		return nil, errors.New("sweep: buffer is required")
	}
	if c == nil {
		return nil, errors.New("sweep: computer is required")
	}
	if e == nil {
		return nil, errors.New("sweep: emitter is required")
	}
	if logger == nil {
		return nil, errors.New("sweep: logger is required")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("sweep: interval must be > 0, got %v", cfg.Interval)
	}
	if cfg.QuietPeriod <= 0 {
		return nil, fmt.Errorf("sweep: quiet_period must be > 0, got %v", cfg.QuietPeriod)
	}
	if cfg.MaxTTL <= 0 {
		return nil, fmt.Errorf("sweep: max_ttl must be > 0, got %v", cfg.MaxTTL)
	}
	if cfg.WorkerPoolSize <= 0 {
		return nil, fmt.Errorf("sweep: worker_pool_size must be > 0, got %v", cfg.WorkerPoolSize)
	}
	return &Sweeper{
		buf:      buf,
		computer: c,
		emitter:  e,
		cfg:      cfg,
		gate:     gate,
		metrics:  m,
		logger:   logger,
		now:      time.Now,
	}, nil
}

func (*Sweeper) Name() string { return "sweeper" }

// Start spawns the tick goroutine on context.Background so a Drain →
// row-collection sequence is not interrupted by SIGTERM; Shutdown signals
// via stopCh.
//
//nolint:gosec,contextcheck // intentional Background-rooted goroutine.
func (s *Sweeper) Start(_ context.Context, _ run.Host) error {
	s.started = true
	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})
	go s.run()
	return nil
}

// Shutdown signals the tick loop and waits for it to exit. The wait is
// bounded by one trace's worst-case Drain + Compute plus the final batch
// Emit (a queue send) — independent of the supplied ctx. Shutdown is the
// sweeper-side R2 exception (ADR §5.3).
func (s *Sweeper) Shutdown(_ context.Context) error {
	s.closeOnce.Do(func() {
		if !s.started {
			return
		}
		close(s.stopCh)
		<-s.doneCh
	})
	return nil
}

func (s *Sweeper) run() {
	defer close(s.doneCh)

	if s.gate != nil {
		select {
		case <-s.stopCh:
			return
		case <-s.gate:
		}
	}

	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		// Prefer stopCh so a closed channel always wins against a
		// ticker fire that may have been buffered during a long tick.
		select {
		case <-s.stopCh:
			return
		default:
		}
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			s.tick(context.Background())
		}
	}
}

func (s *Sweeper) tick(ctx context.Context) {
	now := s.now()
	quietCutoff := now.Add(-s.cfg.QuietPeriod)
	ttlCutoff := now.Add(-s.cfg.MaxTTL)

	finalizable, err := s.buf.Scan(ctx, quietCutoff, ttlCutoff)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		s.metrics.incSweep(resultScanError)
		s.logger.Warn("sweep: scan", zap.Error(err))
		return
	}
	if len(finalizable) == 0 {
		s.metrics.incSweep(resultOK)
		return
	}
	s.logger.Debug("sweep: finalizable found", zap.Int("count", len(finalizable)))

	rows := s.fanout(ctx, finalizable)
	if len(rows) == 0 {
		s.metrics.incSweep(resultOK)
		return
	}
	if err := s.emitter.Emit(ctx, rows); err != nil {
		// emit records drops to topology_emissions_failed_total itself.
		s.logger.Warn("sweep: emit", zap.Int("rows", len(rows)), zap.Error(err))
		s.metrics.incSweep(resultEmitError)
		return
	}
	s.logger.Debug("sweep: batch emitted", zap.Int("rows", len(rows)))
	s.metrics.incSweep(resultOK)
}

// fanout drives a bounded worker pool that pulls finalizable traces
// through a mutex-guarded claim loop. Workers acquire the claim mutex,
// non-blocking peek stopCh, and either claim the next trace + increment
// the cursor or exit. A push-style `select { <-stopCh, jobs <- f }`
// dispatcher cannot guarantee this: with both cases ready Go's select
// picks at random, so a worker that becomes ready after stopCh closed
// could still receive a post-shutdown trace. The pull loop bounds
// post-shutdown claims to at most one per worker — the next claim sees
// the closed stopCh and returns false.
func (s *Sweeper) fanout(ctx context.Context, finalizable []buffer.Finalizable) []topology.RootMetrics {
	workers := min(s.cfg.WorkerPoolSize, len(finalizable))

	var (
		claimMu sync.Mutex
		next    int
	)
	claim := func() (buffer.Finalizable, bool) {
		claimMu.Lock()
		defer claimMu.Unlock()
		select {
		case <-s.stopCh:
			return buffer.Finalizable{}, false
		default:
		}
		if next >= len(finalizable) {
			return buffer.Finalizable{}, false
		}
		f := finalizable[next]
		next++
		return f, true
	}

	var (
		wg    sync.WaitGroup
		rowMu sync.Mutex
		rows  = make([]topology.RootMetrics, 0, len(finalizable))
	)
	for range workers {
		wg.Go(func() {
			for {
				f, ok := claim()
				if !ok {
					return
				}
				if out := s.process(ctx, f); len(out) > 0 {
					rowMu.Lock()
					rows = append(rows, out...)
					rowMu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	return rows
}

// process drives one trace through Drain → Compute, updating per-trace
// metrics. It returns the computed rows only when Emit should carry them.
// A trace with multiple authentic roots produces one row per root.
func (s *Sweeper) process(ctx context.Context, f buffer.Finalizable) []topology.RootMetrics {
	spans, drainErr := s.buf.Drain(ctx, f.TraceID)
	if drainErr != nil {
		s.metrics.incDrainError()
		s.logger.Warn("sweep: drain",
			zap.String("trace_id", hex.EncodeToString(f.TraceID[:])),
			zap.String("trigger", f.Trigger),
			zap.Error(drainErr))
		return nil
	}
	if len(spans) == 0 {
		// Already drained (raced) or expired between Scan and Drain.
		return nil
	}

	start := time.Now()
	rows, orphans, computeErr := s.computer.Compute(f.TraceID, spans)
	s.metrics.observeComputeDuration(time.Since(start))
	s.metrics.addOrphanSpans(orphans)
	s.metrics.addOrphanBytes(orphanBytes(spans, rows))

	if errors.Is(computeErr, ErrNoRoot) {
		s.metrics.incComputeSkipped(reasonNoRoot)
		return nil
	}
	if computeErr != nil {
		s.metrics.incComputeError()
		s.logger.Warn("sweep: compute",
			zap.String("trace_id", hex.EncodeToString(f.TraceID[:])),
			zap.String("trigger", f.Trigger),
			zap.Error(computeErr))
		return nil
	}
	s.metrics.incFinalized(f.Trigger)
	seen := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		seen[r.RootID] = struct{}{}
	}
	if extras := len(rows) - len(seen); extras > 0 {
		s.metrics.addRootIDCollisions(extras)
		s.logger.Warn("sweep: root_id collision within trace",
			zap.String("trace_id", hex.EncodeToString(f.TraceID[:])),
			zap.Int("colliding_rows", extras),
			zap.Int("root_count", len(rows)))
	}
	s.logger.Debug("sweep: trace finalized",
		zap.String("trace_id", hex.EncodeToString(f.TraceID[:])),
		zap.String("trigger", f.Trigger),
		zap.Int("root_count", len(rows)),
		zap.Int32("orphan_count", orphans))
	return rows
}

func orphanBytes(spans map[string]buffer.SpanRecord, rows []topology.RootMetrics) int64 {
	var total int64
	for _, r := range spans {
		total += r.SizeBytes
	}
	var attributed int64
	for _, row := range rows {
		attributed += row.SpanBytesTotal
	}
	if total <= attributed {
		return 0
	}
	return total - attributed
}
