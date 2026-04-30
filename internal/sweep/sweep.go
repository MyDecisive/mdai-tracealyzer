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
	Compute(traceID [16]byte, trigger string, spans map[string]buffer.SpanRecord) ([]topology.RootMetrics, int32, error)
}

// Buffer is the subset of buffer.Buffer that the sweeper depends on. Put
// and Close belong to the ingest and lifecycle paths respectively.
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
	metrics  *Metrics
	logger   *zap.Logger
	now      func() time.Time
}

func New(buf Buffer, c Computer, e Emitter, cfg Config, m *Metrics, logger *zap.Logger) (*Sweeper, error) {
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
		metrics:  m,
		logger:   logger,
		now:      time.Now,
	}, nil
}

func (*Sweeper) Name() string { return "sweeper" }

// Start runs the tick loop until ctx cancels. On cancel it blocks until
// any in-flight tick completes — tick uses context.WithoutCancel
// internally to keep the Drain → Emit pair atomic across SIGTERM.
func (s *Sweeper) Start(ctx context.Context) error {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.tick(ctx)
		}
	}
}

// Stop is a no-op: Start has already cleaned up by the time it returns.
func (*Sweeper) Stop(_ context.Context) error { return nil }

func (s *Sweeper) tick(parent context.Context) {
	now := s.now()
	quietCutoff := now.Add(-s.cfg.QuietPeriod)
	ttlCutoff := now.Add(-s.cfg.MaxTTL)

	finalizable, err := s.buf.Scan(parent, quietCutoff, ttlCutoff)
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

	// Drain deletes state from Valkey; the resulting row must reach Emit even
	// if parent cancels mid-phase.
	ctx := context.WithoutCancel(parent)
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

// fanout runs Drain → Compute across a bounded worker pool. Emit batching
// remains serial in the caller because rows are order-independent within a
// batch and the emitter already owns queueing.
func (s *Sweeper) fanout(ctx context.Context, finalizable []buffer.Finalizable) []topology.RootMetrics {
	workers := min(s.cfg.WorkerPoolSize, len(finalizable))

	jobs := make(chan buffer.Finalizable, len(finalizable))
	for _, f := range finalizable {
		jobs <- f
	}
	close(jobs)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		rows = make([]topology.RootMetrics, 0, len(finalizable))
	)
	for range workers {
		wg.Go(func() {
			for f := range jobs {
				if out := s.process(ctx, f); len(out) > 0 {
					mu.Lock()
					rows = append(rows, out...)
					mu.Unlock()
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
	rows, orphans, computeErr := s.computer.Compute(f.TraceID, f.Trigger, spans)
	s.metrics.observeComputeDuration(time.Since(start))
	s.metrics.addOrphanSpans(orphans)

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
	s.logger.Debug("sweep: trace finalized",
		zap.String("trace_id", hex.EncodeToString(f.TraceID[:])),
		zap.String("trigger", f.Trigger),
		zap.Int("root_count", len(rows)),
		zap.Int32("orphan_count", orphans))
	return rows
}
