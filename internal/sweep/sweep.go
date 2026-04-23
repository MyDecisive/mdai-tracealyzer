package sweep

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/emit"
	"go.uber.org/zap"
)

// ErrNoRoot is the sentinel a Computer returns when a trace has no
// discoverable root span. The sweeper treats it as an expected skip,
// not an error. Any other error from Compute is a compute bug and
// increments the error counter.
var ErrNoRoot = errors.New("no root span")

// Computer implementations return ErrNoRoot (directly or wrapped) when
// no root span is discoverable; any other error is treated as a compute bug.
type Computer interface {
	Compute(traceID [16]byte, trigger string, spans map[string]buffer.SpanRecord) (emit.RootMetrics, error)
}

type Config struct {
	QuietPeriod    time.Duration
	MaxTTL         time.Duration
	Interval       time.Duration
	WorkerPoolSize int
}

// Sweeper drives finalization on a ticker: Scan → Drain → Compute → Emit.
type Sweeper struct {
	buf      buffer.Buffer
	computer Computer
	emitter  emit.Emitter
	cfg      Config
	metrics  *Metrics
	logger   *zap.Logger
	now      func() time.Time
}

func New(buf buffer.Buffer, c Computer, e emit.Emitter, cfg Config, m *Metrics, logger *zap.Logger) (*Sweeper, error) {
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

// Run returns nil on graceful shutdown; a non-nil return would be a
// setup bug (no such path exists today).
func (s *Sweeper) Run(ctx context.Context) error {
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

func (s *Sweeper) tick(parent context.Context) {
	// Run the tick to completion even if parent cancels mid-flight: once Drain
	// removes state from Valkey, the row must reach Emit. Run() still exits
	// promptly because it only re-selects on ctx.Done after tick returns.
	ctx := context.WithoutCancel(parent)

	now := s.now()
	quietCutoff := now.Add(-s.cfg.QuietPeriod)
	ttlCutoff := now.Add(-s.cfg.MaxTTL)

	finalizable, err := s.buf.Scan(ctx, quietCutoff, ttlCutoff)
	if err != nil {
		s.metrics.incSweep(resultScanError)
		s.logger.Warn("sweep: scan", zap.Error(err))
		return
	}
	if len(finalizable) == 0 {
		s.metrics.incSweep(resultOK)
		return
	}

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
	s.metrics.incSweep(resultOK)
}

// fanout runs Drain → Compute across a bounded worker pool. Emit batching
// remains serial in the caller because rows are order-independent within a
// batch and the emitter already owns queueing.
func (s *Sweeper) fanout(ctx context.Context, finalizable []buffer.Finalizable) []emit.RootMetrics {
	workers := s.cfg.WorkerPoolSize
	if workers > len(finalizable) {
		workers = len(finalizable)
	}

	jobs := make(chan buffer.Finalizable, len(finalizable))
	for _, f := range finalizable {
		jobs <- f
	}
	close(jobs)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		rows = make([]emit.RootMetrics, 0, len(finalizable))
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range jobs {
				if rm, ok := s.process(ctx, f); ok {
					mu.Lock()
					rows = append(rows, rm)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return rows
}

// process drives one trace through Drain → Compute, updating per-trace
// metrics. It returns the computed row only when Emit should carry it.
func (s *Sweeper) process(ctx context.Context, f buffer.Finalizable) (emit.RootMetrics, bool) {
	spans, drainErr := s.buf.Drain(ctx, f.TraceID)
	if drainErr != nil {
		s.metrics.incDrainError()
		s.logger.Warn("sweep: drain",
			zap.String("trace_id", hex.EncodeToString(f.TraceID[:])),
			zap.String("trigger", f.Trigger),
			zap.Error(drainErr))
		return emit.RootMetrics{}, false
	}
	if len(spans) == 0 {
		// Already drained (raced) or expired between Scan and Drain.
		return emit.RootMetrics{}, false
	}

	rm, computeErr := s.computer.Compute(f.TraceID, f.Trigger, spans)
	if errors.Is(computeErr, ErrNoRoot) {
		s.metrics.incComputeSkipped(reasonNoRoot)
		return emit.RootMetrics{}, false
	}
	if computeErr != nil {
		s.metrics.incComputeError()
		s.logger.Warn("sweep: compute",
			zap.String("trace_id", hex.EncodeToString(f.TraceID[:])),
			zap.String("trigger", f.Trigger),
			zap.Error(computeErr))
		return emit.RootMetrics{}, false
	}
	s.metrics.incFinalized(f.Trigger)
	return rm, true
}
