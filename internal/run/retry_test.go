package run_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"go.uber.org/zap"
)

func TestRetry_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	err := run.Retry(t.Context(), "probe", func(context.Context) error {
		calls.Add(1)
		return nil
	}, run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls: want 1, got %d", calls.Load())
	}
}

func TestRetry_FailureThenSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	err := run.Retry(t.Context(), "probe", func(context.Context) error {
		if calls.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	}, run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls: want 2, got %d", calls.Load())
	}
}

func TestRetry_CancelDuringBackoffReturnsCtxErr(t *testing.T) {
	t.Parallel()

	opErr := errors.New("transient")
	ctx, cancel := context.WithCancel(t.Context())

	var calls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- run.Retry(ctx, "probe", func(context.Context) error {
			calls.Add(1)
			return opErr
		}, run.Backoff{Initial: time.Hour, Max: time.Hour}, zap.NewNop())
	}()

	// Wait until at least one op call has happened so we know Retry is in
	// the backoff sleep.
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err must wrap context.Canceled: %v", err)
	}
}

func TestRetry_AlreadyCancelledShortCircuits(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	err := run.Retry(ctx, "probe", func(context.Context) error {
		return errors.New("transient")
	}, run.Backoff{Initial: time.Hour, Max: time.Hour}, zap.NewNop())
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err must wrap context.Canceled: %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("must short-circuit on already-cancelled ctx; elapsed=%v", elapsed)
	}
}

func TestRetry_PermanentErrorStopsRetries(t *testing.T) {
	t.Parallel()

	permErr := errors.New("schema is broken")
	var calls atomic.Int32
	err := run.Retry(t.Context(), "probe", func(context.Context) error {
		calls.Add(1)
		return backoff.Permanent(permErr)
	}, run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())

	if !errors.Is(err, permErr) {
		t.Fatalf("err must wrap permErr: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls: want 1, got %d", calls.Load())
	}
}

func TestRetry_ZeroInitialIsRejected(t *testing.T) {
	t.Parallel()

	err := run.Retry(t.Context(), "probe", func(context.Context) error {
		return nil
	}, run.Backoff{}, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for zero Initial")
	}
}
