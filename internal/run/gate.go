package run

import "context"

// Gated wraps a Component so its Start blocks until a gate channel
// closes. If ctx cancels first, Start returns nil without invoking inner.
// Stop forwards unconditionally so a shutdown during the wait still runs
// inner cleanup.
type Gated struct {
	inner Component
	gate  <-chan struct{}
}

func NewGated(inner Component, gate <-chan struct{}) *Gated {
	return &Gated{inner: inner, gate: gate}
}

func (g *Gated) Name() string { return g.inner.Name() }

func (g *Gated) Start(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-g.gate:
	}
	return g.inner.Start(ctx)
}

func (g *Gated) Stop(ctx context.Context) error { return g.inner.Stop(ctx) }
