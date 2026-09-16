package run_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRetry_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	err := run.Retry(t.Context(), "probe", func(context.Context) error {
		calls.Add(1)
		return nil
	}, run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load(), "calls")
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
	require.NoError(t, err)
	require.Equal(t, int32(2), calls.Load(), "calls")
}

func TestRetry_CancelDuringBackoffReturnsCtxErr(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

		synctest.Wait() // returns once Retry is parked in the backoff sleep
		require.NotZero(t, calls.Load(), "Retry did not call op before entering backoff")
		cancel()

		err := <-done
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestRetry_AlreadyCancelledShortCircuits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		start := time.Now()
		err := run.Retry(ctx, "probe", func(context.Context) error {
			return errors.New("transient")
		}, run.Backoff{Initial: time.Hour, Max: time.Hour}, zap.NewNop())
		elapsed := time.Since(start)

		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, elapsed, "must short-circuit without sleeping the backoff")
	})
}

func TestRetry_PermanentErrorStopsRetries(t *testing.T) {
	t.Parallel()

	permErr := errors.New("schema is broken")
	var calls atomic.Int32
	err := run.Retry(t.Context(), "probe", func(context.Context) error {
		calls.Add(1)
		return backoff.Permanent(permErr)
	}, run.Backoff{Initial: time.Millisecond, Max: time.Millisecond}, zap.NewNop())

	require.ErrorIs(t, err, permErr)
	require.Equal(t, int32(1), calls.Load(), "calls")
}

func TestRetry_ZeroInitialIsRejected(t *testing.T) {
	t.Parallel()

	err := run.Retry(t.Context(), "probe", func(context.Context) error {
		return nil
	}, run.Backoff{}, zap.NewNop())
	require.Error(t, err, "zero Initial must be rejected")
}
