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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

		require.NoError(t, probe.Start(t.Context(), &recordingHost{}))
		synctest.Wait()
		require.NoError(t, probe.Shutdown(t.Context()))
		assert.Equal(t, int32(1), ready.Load(), "onReady calls")
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

		require.NoError(t, probe.Start(t.Context(), &recordingHost{}))
		<-ready // clock auto-advances through each backoff until the check succeeds
		require.NoError(t, probe.Shutdown(t.Context()))
		assert.Equal(t, int32(3), calls.Load(), "check calls")
	})
}

func TestProbe_NoOnReadyIsHarmless(t *testing.T) {
	t.Parallel()
	probe := run.NewProbe("dep",
		func(context.Context) error { return nil },
		nil, fastBackoff(), zap.NewNop())

	require.NoError(t, probe.Start(t.Context(), &recordingHost{}))
	require.NoError(t, probe.Shutdown(t.Context()))
}

func TestProbe_NonRetryableErrorEscalatesToHostFatal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		permErr := errors.New("permanent")
		host := &recordingHost{}
		probe := run.NewProbe("dep",
			func(context.Context) error { return backoff.Permanent(permErr) },
			func() { assert.Fail(t, "onReady must not run on permanent error") },
			fastBackoff(), zap.NewNop())

		require.NoError(t, probe.Start(t.Context(), host))
		synctest.Wait()
		require.NoError(t, probe.Shutdown(t.Context()))
		calls := host.snapshot()
		require.Len(t, calls, 1, "Fatal calls")
		assert.Equal(t, "probe:dep", calls[0].component, "Fatal component")
		require.ErrorIs(t, calls[0].err, permErr, "Fatal err")
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
			func() { assert.Fail(t, "onReady must not run") },
			run.Backoff{Initial: time.Hour, Max: time.Hour}, zap.NewNop())

		require.NoError(t, probe.Start(t.Context(), &recordingHost{}))
		synctest.Wait() // parked in backoff after the first failed attempt
		atShutdown := calls.Load()
		require.NoError(t, probe.Shutdown(t.Context()))
		synctest.Wait()
		assert.Equal(t, atShutdown, calls.Load(), "retry continued after Shutdown")
	})
}

func TestProbe_ShutdownWithoutStartIsNoop(t *testing.T) {
	t.Parallel()
	probe := run.NewProbe("dep",
		func(context.Context) error { return nil },
		nil, fastBackoff(), zap.NewNop())

	require.NoError(t, probe.Shutdown(t.Context()), "Shutdown without Start must return nil")
}

func TestProbe_ShutdownIsIdempotent(t *testing.T) {
	t.Parallel()
	probe := run.NewProbe("dep",
		func(context.Context) error { return nil },
		nil, fastBackoff(), zap.NewNop())

	require.NoError(t, probe.Start(t.Context(), &recordingHost{}))
	require.NoError(t, probe.Shutdown(t.Context()), "first Shutdown")
	require.NoError(t, probe.Shutdown(t.Context()), "second Shutdown")
}
