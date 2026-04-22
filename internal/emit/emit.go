package emit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// ErrClosed is returned when Emit is called after Close.
	ErrClosed = errors.New("emitter closed")
	// ErrQueueFull is returned when rows cannot be accepted because the queue is full.
	ErrQueueFull = errors.New("emitter queue full")
)

const loggedTraceIDLimit = 10

// RootMetrics is the emitter input contract for the first implementation cut.
type RootMetrics struct {
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

// Emitter accepts finalized topology rows for eventual emission to GreptimeDB.
type Emitter interface {
	Emit(ctx context.Context, rows []RootMetrics) error
	Close(ctx context.Context) error
}

type writer interface {
	Write(ctx context.Context, batch writeBatch) error
	Close() error
}

// TODO: inject RootMetrics struct?
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

type emitter struct {
	cfg     config.Emitter
	logger  *zap.Logger
	metrics metrics
	writer  writer
	now     func() time.Time
	sleep   func(context.Context, time.Duration) error

	mu     sync.Mutex
	closed bool

	queue  chan []RootMetrics
	stopCh chan context.Context
	doneCh chan struct{}
	wg     sync.WaitGroup

	errMu   sync.Mutex
	lastErr error
}

// New constructs the production emitter using the GreptimeDB SDK client.
func New(cfg config.Emitter, logger *zap.Logger, reg prometheus.Registerer) (Emitter, error) {
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

	return newWithWriter(cfg, logger, m, w, time.Now, sleepContext), nil
}

func newWithWriter(
	cfg config.Emitter,
	logger *zap.Logger,
	m metrics,
	w writer,
	now func() time.Time,
	sleep func(context.Context, time.Duration) error,
) *emitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = sleepContext
	}

	e := &emitter{
		cfg:     cfg,
		logger:  logger,
		metrics: m,
		writer:  w,
		now:     now,
		sleep:   sleep,
		queue:   make(chan []RootMetrics, cfg.QueueCapacity),
		stopCh:  make(chan context.Context, 1),
		doneCh:  make(chan struct{}),
	}

	e.wg.Add(1)
	go e.run()

	return e
}

func (e *emitter) Emit(_ context.Context, rows []RootMetrics) error {
	if len(rows) == 0 {
		return nil
	}

	// TODO: remove cloning if we can be sure that:
	//  After calling Emit(ctx, rows), the caller transfers ownership of the slice contents to the emitter until the emitter has processed them. The caller must not mutate the slice elements or reuse the slice backing
	//  array.
	cloned := cloneRows(rows)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	select {
	case e.queue <- cloned:
		return nil
	default:
		e.recordDroppedRows("queue full", cloned, ErrQueueFull)
		return ErrQueueFull
	}
}

func (e *emitter) Close(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		select {
		case <-e.doneCh:
			return e.closeErr()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.closed = true
	e.mu.Unlock()

	e.stopCh <- ctx

	select {
	case <-e.doneCh:
		return e.closeErr()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *emitter) run() {
	defer e.wg.Done()
	defer close(e.doneCh)

	ticker := time.NewTicker(e.cfg.FlushInterval.Duration())
	defer ticker.Stop()

	pending := make([]RootMetrics, 0, e.cfg.BatchSize)

	for {
		select {
		case rows := <-e.queue:
			pending = append(pending, rows...)
			if err := e.flushReady(context.Background(), &pending); err != nil {
				e.setErr(err)
				e.logger.Error("flush emitter batch", zap.Error(err))
			}
		case <-ticker.C:
			if err := e.flushAll(context.Background(), &pending); err != nil {
				e.setErr(err)
				e.logger.Error("flush emitter batch", zap.Error(err))
			}
		case shutdownCtx := <-e.stopCh:
			pending = e.drainQueue(pending)
			if err := e.flushAll(shutdownCtx, &pending); err != nil {
				e.setErr(err)
				e.logger.Error("flush emitter batch during shutdown", zap.Error(err))
			}
			if err := e.writer.Close(); err != nil {
				e.setErr(err)
				e.logger.Error("close emitter writer", zap.Error(err))
			}
			return
		}
	}
}

func (e *emitter) drainQueue(pending []RootMetrics) []RootMetrics {
	for {
		select {
		case rows := <-e.queue:
			pending = append(pending, rows...)
		default:
			return pending
		}
	}
}

func (e *emitter) flushReady(parent context.Context, pending *[]RootMetrics) error {
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

func (e *emitter) flushAll(parent context.Context, pending *[]RootMetrics) error {
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

func (e *emitter) writeWithRetry(parent context.Context, batch []RootMetrics) error {
	if len(batch) == 0 {
		return nil
	}

	for attempt := 0; ; attempt++ {
		ctx, cancel := context.WithTimeout(parent, e.cfg.Timeout.Duration())
		err := e.writer.Write(ctx, makeWriteBatch(e.cfg.TableName, batch, e.now()))
		cancel()
		if err == nil {
			return nil
		}

		if attempt >= e.cfg.MaxRetries {
			e.recordDroppedRows("write retries exhausted", batch, err)
			return err
		}

		backoff := e.cfg.InitialBackoff.Duration() * time.Duration(1<<attempt)
		if sleepErr := e.sleep(parent, backoff); sleepErr != nil {
			e.recordDroppedRows("write retries canceled", batch, err)
			return fmt.Errorf("wait for retry: %w", sleepErr)
		}
	}
}

func (e *emitter) recordDroppedRows(reason string, rows []RootMetrics, err error) {
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

func makeWriteBatch(tableName string, rows []RootMetrics, ts time.Time) writeBatch {
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
		Table: tableName,
		Rows:  out,
	}
}

func cloneRows(rows []RootMetrics) []RootMetrics {
	out := make([]RootMetrics, len(rows))
	copy(out, rows)
	return out
}

func boundedTraceIDs(rows []RootMetrics) []string {
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
	// TODO: this nil check not needed?
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

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (e *emitter) setErr(err error) {
	if err == nil {
		return
	}
	e.errMu.Lock()
	defer e.errMu.Unlock()
	if e.lastErr == nil {
		e.lastErr = err
	}
}

func (e *emitter) closeErr() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()
	return e.lastErr
}
