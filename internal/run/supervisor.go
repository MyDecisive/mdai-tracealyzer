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

func New(grace time.Duration, logger *zap.Logger, components ...Component) *Supervisor {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Supervisor{
		components: components,
		grace:      grace,
		logger:     logger,
		preStop:    func() {},
	}
}

// OnShutdown registers a hook invoked once, after Start has unblocked and
// before any Stop call.
func (s *Supervisor) OnShutdown(fn func()) {
	s.preStop = fn
}

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

	waited := make(chan error, 1)
	go func() { waited <- g.Wait() }()

	var runErr error
	select {
	case runErr = <-waited:
	case <-ctx.Done():
		select {
		case runErr = <-waited:
		case <-time.After(s.grace):
			s.logger.Warn("supervisor: components did not exit within grace; proceeding to shutdown",
				zap.Duration("grace", s.grace))
			runErr = ctx.Err()
		}
	}
	if runErr != nil {
		s.logger.Error("supervisor: component exited with error", zap.Error(runErr))
	} else {
		s.logger.Info("supervisor: all components exited cleanly")
	}

	s.preStop()

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
