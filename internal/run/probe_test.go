package run_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"go.uber.org/zap"
)

type fatalCall struct {
	component string
	err       error
}

type recordingHost struct {
	mu    sync.Mutex
	calls []fatalCall
}

func (h *recordingHost) Fatal(component string, err error) {
	h.mu.Lock()
	h.calls = append(h.calls, fatalCall{component: component, err: err})
	h.mu.Unlock()
}

func (h *recordingHost) snapshot() []fatalCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]fatalCall, len(h.calls))
	copy(out, h.calls)
	return out
}

func fastBackoff() run.Backoff {
	return run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}
}

func TestProbe_OnReadyCalledAfterCheckSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var ready atomic.Int32
		probe := run.NewProbe("dep",
			func(context.Context) error { return nil },
			func() { ready.Add(1) },
			fastBackoff(), zap.NewNop())

		if err := probe.Start(t.Context(), &recordingHost{}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		synctest.Wait()
		if err := probe.Shutdown(t.Context()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if ready.Load() != 1 {
			t.Errorf("onReady calls: want 1, got %d", ready.Load())
		}
	})
}

func TestProbe_RetriesUntilSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		ready := make(chan struct{})
		probe := run.NewProbe("dep",
			func(context.Context) error {
				if calls.Add(1) < 3 {
					return errors.New("not yet")
				}
				return nil
			},
			func() { close(ready) },
			fastBackoff(), zap.NewNop())

		if err := probe.Start(t.Context(), &recordingHost{}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		<-ready // clock auto-advances through each backoff until the check succeeds
		if err := probe.Shutdown(t.Context()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if calls.Load() != 3 {
			t.Errorf("check calls: want 3, got %d", calls.Load())
		}
	})
}

func TestProbe_NoOnReadyIsHarmless(t *testing.T) {
	t.Parallel()
	probe := run.NewProbe("dep",
		func(context.Context) error { return nil },
		nil, fastBackoff(), zap.NewNop())

	if err := probe.Start(t.Context(), &recordingHost{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := probe.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestProbe_NonRetryableErrorEscalatesToHostFatal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		permErr := errors.New("permanent")
		host := &recordingHost{}
		probe := run.NewProbe("dep",
			func(context.Context) error { return backoff.Permanent(permErr) },
			func() { t.Error("onReady must not run on permanent error") },
			fastBackoff(), zap.NewNop())

		if err := probe.Start(t.Context(), host); err != nil {
			t.Fatalf("Start: %v", err)
		}
		synctest.Wait()
		if err := probe.Shutdown(t.Context()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		calls := host.snapshot()
		if len(calls) != 1 {
			t.Fatalf("Fatal calls: want 1, got %d", len(calls))
		}
		if calls[0].component != "probe:dep" {
			t.Errorf("Fatal component: want probe:dep, got %s", calls[0].component)
		}
		if !errors.Is(calls[0].err, permErr) {
			t.Errorf("Fatal err: want wraps %v, got %v", permErr, calls[0].err)
		}
	})
}

func TestProbe_ShutdownCancelsRunningRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		probe := run.NewProbe("dep",
			func(context.Context) error {
				calls.Add(1)
				return errors.New("never succeeds")
			},
			func() { t.Error("onReady must not run") },
			run.Backoff{Initial: time.Hour, Max: time.Hour}, zap.NewNop())

		if err := probe.Start(t.Context(), &recordingHost{}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		synctest.Wait() // parked in backoff after the first failed attempt
		atShutdown := calls.Load()
		if err := probe.Shutdown(t.Context()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		synctest.Wait()
		if calls.Load() != atShutdown {
			t.Errorf("retry continued after Shutdown: %d → %d", atShutdown, calls.Load())
		}
	})
}

func TestProbe_ShutdownWithoutStartIsNoop(t *testing.T) {
	t.Parallel()
	probe := run.NewProbe("dep",
		func(context.Context) error { return nil },
		nil, fastBackoff(), zap.NewNop())

	if err := probe.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown without Start must return nil, got %v", err)
	}
}

func TestProbe_ShutdownIsIdempotent(t *testing.T) {
	t.Parallel()
	probe := run.NewProbe("dep",
		func(context.Context) error { return nil },
		nil, fastBackoff(), zap.NewNop())

	if err := probe.Start(t.Context(), &recordingHost{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := probe.Shutdown(t.Context()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := probe.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}
