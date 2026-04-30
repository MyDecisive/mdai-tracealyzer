package run

import (
	"context"

	"go.uber.org/zap"
)

// Probe runs a readiness check until it succeeds or ctx is cancelled. It
// is a fire-and-exit Component: once the underlying dependency reports
// healthy, Probe.Start invokes onReady and returns nil. The supervisor
// keeps the process up for the remaining components.
//
// Probe never gives up on its own — Kubernetes startup-probe budget is the
// authoritative "give up and restart the pod" signal. Treat ctx
// cancellation as a graceful supervisor shutdown, not a probe failure, so
// Start returns nil rather than propagating the wrapped ctx error.
type Probe struct {
	name    string
	check   func(context.Context) error
	onReady func()
	backoff Backoff
	logger  *zap.Logger
}

// NewProbe constructs a Probe. onReady may be nil.
func NewProbe(name string, check func(context.Context) error, onReady func(), b Backoff, logger *zap.Logger) *Probe {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Probe{
		name:    name,
		check:   check,
		onReady: onReady,
		backoff: b,
		logger:  logger,
	}
}

func (p *Probe) Name() string { return "probe:" + p.name }

func (p *Probe) Start(ctx context.Context) error {
	err := Retry(ctx, p.name, p.check, p.backoff, p.logger)
	if err == nil {
		if p.onReady != nil {
			p.onReady()
		}
		p.logger.Info("probe ready", zap.String("name", p.name))
		return nil
	}
	if ctx.Err() != nil {
		return nil //nolint:nilerr // ctx cancel is supervisor shutdown, not probe failure.
	}
	return err
}

// Stop is a no-op: Start exits on ctx cancellation or success.
func (*Probe) Stop(_ context.Context) error { return nil }
