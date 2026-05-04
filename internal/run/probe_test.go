package run_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"go.uber.org/zap"
)

func TestProbe_ImmediateSuccessCallsOnReady(t *testing.T) {
	t.Parallel()

	var ready atomic.Int32
	probe := run.NewProbe("dep", func(context.Context) error {
		return nil
	}, func() { ready.Add(1) },
		run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())

	if err := probe.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if ready.Load() != 1 {
		t.Fatalf("onReady calls: want 1, got %d", ready.Load())
	}
}

func TestProbe_RetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var ready atomic.Int32
	probe := run.NewProbe("dep", func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("not yet")
		}
		return nil
	}, func() { ready.Add(1) },
		run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())

	if err := probe.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls: want 3, got %d", calls.Load())
	}
	if ready.Load() != 1 {
		t.Fatal("onReady not called")
	}
}

func TestProbe_CtxCancelReturnsNilWithoutOnReady(t *testing.T) {
	t.Parallel()

	var ready atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())

	probe := run.NewProbe("dep", func(context.Context) error {
		return errors.New("never")
	}, func() { ready.Add(1) },
		run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())

	done := make(chan error, 1)
	go func() { done <- probe.Start(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start must return nil on ctx cancel, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return on ctx cancel")
	}
	if ready.Load() != 0 {
		t.Fatal("onReady must not be called when probe never succeeds")
	}
}

func TestProbe_NoOnReadyIsHarmless(t *testing.T) {
	t.Parallel()

	probe := run.NewProbe("dep", func(context.Context) error { return nil }, nil,
		run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())
	if err := probe.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}
