package run

import (
	"context"

	"go.uber.org/zap"
)

type Probe struct {
	name    string
	check   func(context.Context) error
	onReady func()
	backoff Backoff
	logger  *zap.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

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

// Start roots the retry goroutine on Background so SIGTERM doesn't abort it.
//
//nolint:contextcheck,unparam
func (p *Probe) Start(_ context.Context, host Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		err := Retry(ctx, p.name, p.check, p.backoff, p.logger)
		if err == nil {
			if p.onReady != nil {
				p.onReady()
			}
			p.logger.Info("probe ready", zap.String("name", p.name))
			return
		}
		if ctx.Err() != nil {
			return
		}
		host.Fatal("probe:"+p.name, err)
	}()
	return nil
}

func (p *Probe) Shutdown(ctx context.Context) error {
	if p.cancel == nil {
		return nil
	}
	p.cancel()
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
