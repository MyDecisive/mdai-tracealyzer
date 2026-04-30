package run

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Supervisor orchestrates a fixed set of Components: it starts them
// concurrently, waits for the first failure or for ctx to cancel, then
// stops every component in reverse registration order under a single
// shutdown deadline. Stop runs even when a component's Start returned an
// error so resources are released regardless of the failure path.
type Supervisor struct {
	components []Component
	grace      time.Duration
	logger     *zap.Logger
	preStop    func()
}

// New: Start order is registration order; Stop runs in reverse. grace
// bounds the cumulative Stop phase.
func New(grace time.Duration, logger *zap.Logger, components ...Component) *Supervisor {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Supervisor{
		components: components,
		grace:      grace,
		logger:     logger,
	}
}

// OnShutdown registers a hook invoked once, after Start has unblocked and
// before any Stop call.
func (s *Supervisor) OnShutdown(fn func()) {
	s.preStop = fn
}

// Run starts every component, blocks until the first one returns or ctx
// cancels, then stops every component under a fresh shutdown context. The
// returned error joins the run-phase error with any stop-phase errors.
func (s *Supervisor) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, c := range s.components {
		s.logger.Info("supervisor: starting component", zap.String("name", c.Name()))
		g.Go(func() error {
			if err := c.Start(gctx); err != nil {
				return fmt.Errorf("%s: %w", c.Name(), err)
			}
			return nil
		})
	}

	runErr := g.Wait()
	if runErr != nil {
		s.logger.Error("supervisor: component exited with error", zap.Error(runErr))
	} else {
		s.logger.Info("supervisor: all components exited cleanly")
	}

	if s.preStop != nil {
		s.preStop()
	}

	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.grace)
	defer cancel()

	stopErrs := s.stopAll(stopCtx)
	if len(stopErrs) > 0 {
		return errors.Join(append([]error{runErr}, stopErrs...)...)
	}
	return runErr
}

func (s *Supervisor) stopAll(ctx context.Context) []error {
	var errs []error
	for i := len(s.components) - 1; i >= 0; i-- {
		c := s.components[i]
		s.logger.Info("supervisor: stopping component", zap.String("name", c.Name()))
		if err := c.Stop(ctx); err != nil {
			s.logger.Warn("supervisor: stop failed",
				zap.String("name", c.Name()), zap.Error(err))
			errs = append(errs, fmt.Errorf("stop %s: %w", c.Name(), err))
		}
	}
	return errs
}
