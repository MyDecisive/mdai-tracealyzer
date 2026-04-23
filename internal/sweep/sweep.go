package sweep

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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
	QuietPeriod time.Duration
	MaxTTL      time.Duration
	Interval    time.Duration
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

func (s *Sweeper) tick(ctx context.Context) {
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

	rows := make([]emit.RootMetrics, 0, len(finalizable))
	for _, f := range finalizable {
		traceHex := hex.EncodeToString(f.TraceID[:])

		spans, drainErr := s.buf.Drain(ctx, f.TraceID)
		if drainErr != nil {
			s.metrics.incDrainError()
			s.logger.Warn("sweep: drain",
				zap.String("trace_id", traceHex),
				zap.String("trigger", f.Trigger),
				zap.Error(drainErr))
			continue
		}
		if len(spans) == 0 {
			// Already drained (raced) or expired between Scan and Drain.
			continue
		}

		rm, computeErr := s.computer.Compute(f.TraceID, f.Trigger, spans)
		if errors.Is(computeErr, ErrNoRoot) {
			s.metrics.incComputeSkipped(reasonNoRoot)
			continue
		}
		if computeErr != nil {
			s.metrics.incComputeError()
			s.logger.Warn("sweep: compute",
				zap.String("trace_id", traceHex),
				zap.String("trigger", f.Trigger),
				zap.Error(computeErr))
			continue
		}
		s.metrics.incFinalized(f.Trigger)
		rows = append(rows, rm)
	}

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
