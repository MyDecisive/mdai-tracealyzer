package emit

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/greptimecfg"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// ErrClosed is returned when Emit is called after Stop.
	ErrClosed = errors.New("emitter closed")
	// ErrQueueFull is returned when rows cannot be accepted because the queue is full.
	ErrQueueFull = errors.New("emitter queue full")
)

const loggedTraceIDLimit = 10

type writer interface {
	Write(ctx context.Context, batch writeBatch) error
	HealthCheck(ctx context.Context) error
	Close() error
}

type row struct {
	Timestamp       time.Time
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

type writeBatch struct {
	Table string
	Rows  []row
}

type metrics struct {
	emissionsFailed prometheus.Counter
}

// Emitter accepts finalized topology rows and ships them to GreptimeDB
// asynchronously. It is a run.Component: Start runs the flush loop, Stop
// drains pending rows and closes the underlying writer.
type Emitter struct {
	cfg     config.Emitter
	logger  *zap.Logger
	metrics metrics
	writer  writer
	now     func() time.Time

	queue   chan []topology.RootMetrics
	pending []topology.RootMetrics
	closed  atomic.Bool
}

func New(cfg config.Emitter, logger *zap.Logger, reg prometheus.Registerer) (*Emitter, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	m, err := newMetrics(reg)
	if err != nil {
		return nil, err
	}

	w, err := newGreptimeWriter(cfg, logger)
	if err != nil {
		return nil, err
	}

	return newWithWriter(cfg, logger, m, w, time.Now), nil
}

func newWithWriter(
	cfg config.Emitter,
	logger *zap.Logger,
	m metrics,
	w writer,
	now func() time.Time,
) *Emitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	if now == nil {
		now = time.Now
	}

	return &Emitter{
		cfg:     cfg,
		logger:  logger,
		metrics: m,
		writer:  w,
		now:     now,
		queue:   make(chan []topology.RootMetrics, cfg.QueueCapacity),
	}
}

func (*Emitter) Name() string { return "emitter" }

func (e *Emitter) HealthCheck(ctx context.Context) error {
	return e.writer.HealthCheck(ctx)
}

// Start runs the flush loop until ctx cancels. Writes use a detached
// context.Background() parent so a SIGTERM does not abort an in-flight
// flush mid-call; Stop performs the final drain under the supervisor's
// shutdown deadline.
//
//nolint:contextcheck // see doc — writes use Background to survive parent cancel.
func (e *Emitter) Start(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.FlushInterval.Duration())
	defer ticker.Stop()

	writeCtx := context.Background()

	for {
		select {
		case <-ctx.Done():
			return nil
		case rows := <-e.queue:
			e.pending = append(e.pending, rows...)
			if err := e.flushReady(writeCtx, &e.pending); err != nil {
				e.logger.Error("flush emitter batch", zap.Error(err))
			}
		case <-ticker.C:
			if err := e.flushAll(writeCtx, &e.pending); err != nil {
				e.logger.Error("flush emitter batch", zap.Error(err))
			}
		}
	}
}

// Stop is idempotent.
func (e *Emitter) Stop(ctx context.Context) error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}

	e.pending = e.drainQueue(e.pending)

	var errs []error
	if err := e.flushAll(ctx, &e.pending); err != nil {
		errs = append(errs, fmt.Errorf("flush during shutdown: %w", err))
	}
	if err := e.writer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close writer: %w", err))
	}
	return errors.Join(errs...)
}

// Emit enqueues rows non-blockingly. Returns ErrClosed after Stop and
// ErrQueueFull when the queue is at capacity.
func (e *Emitter) Emit(_ context.Context, rows []topology.RootMetrics) error {
	if len(rows) == 0 {
		return nil
	}
	if e.closed.Load() {
		return ErrClosed
	}

	// TODO: remove cloning if we can be sure that:
	//  After calling Emit(ctx, rows), the caller transfers ownership of the
	//  slice contents to the emitter until the emitter has processed them.
	//  The caller must not mutate the slice elements or reuse the slice
	//  backing array.
	cloned := cloneRows(rows)

	select {
	case e.queue <- cloned:
		return nil
	default:
		e.recordDroppedRows("queue full", cloned, ErrQueueFull)
		return ErrQueueFull
	}
}

func (e *Emitter) drainQueue(pending []topology.RootMetrics) []topology.RootMetrics {
	for {
		select {
		case rows := <-e.queue:
			pending = append(pending, rows...)
		default:
			return pending
		}
	}
}

func (e *Emitter) flushReady(parent context.Context, pending *[]topology.RootMetrics) error {
	var firstErr error
	for len(*pending) >= e.cfg.BatchSize {
		chunk := cloneRows((*pending)[:e.cfg.BatchSize])
		if err := e.writeWithRetry(parent, chunk); err != nil && firstErr == nil {
			firstErr = err
		}
		*pending = (*pending)[e.cfg.BatchSize:]
	}
	return firstErr
}

func (e *Emitter) flushAll(parent context.Context, pending *[]topology.RootMetrics) error {
	if len(*pending) == 0 {
		return nil
	}

	var firstErr error
	for len(*pending) > 0 {
		size := min(len(*pending), e.cfg.BatchSize)
		chunk := cloneRows((*pending)[:size])
		if err := e.writeWithRetry(parent, chunk); err != nil && firstErr == nil {
			firstErr = err
		}
		*pending = (*pending)[size:]
	}
	return firstErr
}

func (e *Emitter) writeWithRetry(parent context.Context, batch []topology.RootMetrics) error {
	if len(batch) == 0 {
		return nil
	}

	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = e.cfg.InitialBackoff.Duration()
	eb.RandomizationFactor = 0
	eb.Multiplier = 2

	attempt := 0
	var lastWriteErr error
	_, err := backoff.Retry(parent, func() (struct{}, error) {
		ctx, cancel := context.WithTimeout(parent, e.cfg.Timeout.Duration())
		defer cancel()
		werr := e.writer.Write(ctx, makeWriteBatch(batch, e.now()))
		if werr == nil {
			e.logger.Debug("emit: batch written",
				zap.String("table", greptimecfg.SourceTableName),
				zap.Int("rows", len(batch)),
				zap.Int("attempt", attempt))
			return struct{}{}, nil
		}
		attempt++
		lastWriteErr = werr
		return struct{}{}, werr
	},
		backoff.WithBackOff(eb),
		//nolint:gosec // config validation rejects negative MaxRetries; sum >= 1.
		backoff.WithMaxTries(uint(e.cfg.MaxRetries+1)),
	)
	if err == nil {
		return nil
	}
	if parent.Err() != nil {
		e.recordDroppedRows("write retries canceled", batch, lastWriteErr)
		return fmt.Errorf("wait for retry: %w", err)
	}
	e.recordDroppedRows("write retries exhausted", batch, err)
	return err
}

func (e *Emitter) recordDroppedRows(reason string, rows []topology.RootMetrics, err error) {
	e.metrics.emissionsFailed.Add(float64(len(rows)))
	fields := []zap.Field{
		zap.String("reason", reason),
		zap.Int("dropped_rows", len(rows)),
		zap.Int("queue_capacity", e.cfg.QueueCapacity),
		zap.Strings("trace_ids", boundedTraceIDs(rows)),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	e.logger.Warn("drop topology rows", fields...)
}

func makeWriteBatch(rows []topology.RootMetrics, ts time.Time) writeBatch {
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		out = append(out, row{
			Timestamp:       ts,
			RootID:          r.RootID,
			TraceID:         r.TraceID,
			RootService:     r.RootService,
			RootOperation:   r.RootOperation,
			Breadth:         r.Breadth,
			ServiceHopDepth: r.ServiceHopDepth,
			ServiceCount:    r.ServiceCount,
			OperationCount:  r.OperationCount,
			SpanCount:       r.SpanCount,
			ErrorCount:      r.ErrorCount,
			RootDurationNS:  r.RootDurationNS,
		})
	}
	return writeBatch{
		Table: greptimecfg.SourceTableName,
		Rows:  out,
	}
}

func cloneRows(rows []topology.RootMetrics) []topology.RootMetrics {
	out := make([]topology.RootMetrics, len(rows))
	copy(out, rows)
	return out
}

func boundedTraceIDs(rows []topology.RootMetrics) []string {
	ids := make([]string, 0, min(len(rows), loggedTraceIDLimit))
	for i := range min(len(rows), loggedTraceIDLimit) {
		ids = append(ids, rows[i].TraceID)
	}
	return ids
}

func newMetrics(reg prometheus.Registerer) (metrics, error) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "topology_emissions_failed_total",
		Help: "Number of topology rows dropped before a successful GreptimeDB write.",
	})
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	if err := reg.Register(counter); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			return metrics{}, fmt.Errorf("register emitter metrics: %w", err)
		}
		existing, ok := already.ExistingCollector.(prometheus.Counter)
		if !ok {
			return metrics{}, fmt.Errorf("register emitter metrics: existing collector has unexpected type %T", already.ExistingCollector)
		}
		counter = existing
	}
	return metrics{emissionsFailed: counter}, nil
}
