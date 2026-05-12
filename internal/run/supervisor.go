package run

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ErrRunAlreadyCalled is returned by Supervisor.Run if it has already been
// invoked. Run is one-shot: fatalCh and component state are not safe to
// reuse across invocations.
var ErrRunAlreadyCalled = errors.New("supervisor: Run already called")

type fatalSignal struct {
	component string
	err       error
}

type Supervisor struct {
	components []Component
	grace      time.Duration
	logger     *zap.Logger
	onShutdown func()
	fatalCh    chan fatalSignal
	overflowMu sync.Mutex
	overflow   []error
	runStarted atomic.Bool
}

func New(grace time.Duration, logger *zap.Logger, components ...Component) *Supervisor {
	if logger == nil {
		logger = zap.NewNop()
	}
	bufSize := max(len(components), 1)
	return &Supervisor{
		components: components,
		grace:      grace,
		logger:     logger,
		onShutdown: func() {},
		fatalCh:    make(chan fatalSignal, bufSize),
	}
}

func (s *Supervisor) OnShutdown(fn func()) {
	s.onShutdown = fn
}

func (s *Supervisor) Fatal(component string, err error) {
	select {
	case s.fatalCh <- fatalSignal{component: component, err: err}:
	default:
		s.overflowMu.Lock()
		s.overflow = append(s.overflow, fmt.Errorf("%s: %w", component, err))
		s.overflowMu.Unlock()
		s.logger.Warn("supervisor: fatal channel full, dropped into overflow",
			zap.String("component", component), zap.Error(err))
	}
}

func (s *Supervisor) Run(ctx context.Context) error {
	if !s.runStarted.CompareAndSwap(false, true) {
		return ErrRunAlreadyCalled
	}

	var triggerErr error

	for _, c := range s.components {
		s.logger.Info("supervisor: starting component", zap.String("name", c.Name()))
		if err := c.Start(ctx, s); err != nil {
			triggerErr = fmt.Errorf("start %s: %w", c.Name(), err)
			s.logger.Error("supervisor: start failed",
				zap.String("name", c.Name()), zap.Error(err))
			break
		}
	}

	if triggerErr == nil {
		select {
		case <-ctx.Done():
			s.logger.Info("supervisor: shutdown trigger", zap.String("trigger", "ctx_cancel"))
		case f := <-s.fatalCh:
			triggerErr = fmt.Errorf("%s: %w", f.component, f.err)
			s.logger.Error("supervisor: shutdown trigger",
				zap.String("trigger", "fatal"),
				zap.String("name", f.component),
				zap.Error(f.err))
		}
	} else {
		s.logger.Info("supervisor: shutdown trigger", zap.String("trigger", "start_error"))
	}

	s.onShutdown()

	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.grace)
	defer cancel()

	stopErrs := s.shutdownComponents(stopCtx)

	if triggerErr == nil && len(stopErrs) == 0 {
		return nil
	}
	parts := make([]error, 0, 1+len(stopErrs))
	if triggerErr != nil {
		parts = append(parts, triggerErr)
	}
	parts = append(parts, stopErrs...)
	return errors.Join(parts...)
}

func (s *Supervisor) shutdownComponents(stopCtx context.Context) []error {
	var stopErrs []error
	for _, c := range slices.Backward(s.components) {
		s.logger.Info("supervisor: shutting down component", zap.String("name", c.Name()))
		started := time.Now()
		err := c.Shutdown(stopCtx)
		dur := time.Since(started)
		if err != nil {
			s.logger.Warn("supervisor: shutdown failed",
				zap.String("name", c.Name()),
				zap.Duration("duration", dur),
				zap.Error(err))
			stopErrs = append(stopErrs, fmt.Errorf("shutdown %s: %w", c.Name(), err))
			continue
		}
		s.logger.Info("supervisor: shutdown complete",
			zap.String("name", c.Name()), zap.Duration("duration", dur))
	}
	for drained := false; !drained; {
		select {
		case f := <-s.fatalCh:
			stopErrs = append(stopErrs, fmt.Errorf("%s: %w", f.component, f.err))
		default:
			drained = true
		}
	}
	s.overflowMu.Lock()
	stopErrs = append(stopErrs, s.overflow...)
	s.overflow = nil
	s.overflowMu.Unlock()
	return stopErrs
}
