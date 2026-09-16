package app_test

import (
	"sync"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/app"
	"github.com/stretchr/testify/require"
)

func TestReadiness_SingleGate(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http")
	require.False(t, r.Ready(), "expected not ready before Mark")
	r.Mark("http")
	require.True(t, r.Ready(), "expected ready after Mark")
}

func TestReadiness_MultipleGates(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http", "valkey", "otlp_grpc")
	r.Mark("http")
	r.Mark("valkey")
	require.False(t, r.Ready(), "expected not ready with one gate outstanding")
	r.Mark("otlp_grpc")
	require.True(t, r.Ready(), "expected ready after all gates marked")
}

func TestReadiness_UnknownGateIsNoop(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http")
	r.Mark("typo")
	require.False(t, r.Ready(), "unknown gate should not flip readiness")
	r.Mark("http")
	require.True(t, r.Ready(), "expected ready after correct gate")
}

func TestReadiness_IdempotentMark(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http", "valkey")
	r.Mark("http")
	r.Mark("http")
	r.Mark("http")
	require.False(t, r.Ready(), "Ready should require all gates, not repeated marks")
	r.Mark("valkey")
	require.True(t, r.Ready(), "expected ready after both unique gates marked")
}

func TestReadiness_ZeroGatesReadyImmediately(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness()
	require.True(t, r.Ready(), "no gates should mean immediately ready")
}

func TestReadiness_Pending_AllGatesSortedWhenNothingMarked(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("valkey", "otlp_grpc", "otlp_http")
	require.Equal(t, []string{"otlp_grpc", "otlp_http", "valkey"}, r.Pending())
}

func TestReadiness_Pending_ShrinksAsMarksLand(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("a", "b", "c")
	r.Mark("b")
	require.Equal(t, []string{"a", "c"}, r.Pending())
}

func TestReadiness_Pending_NilWhenFullyReady(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("a")
	r.Mark("a")
	require.Nil(t, r.Pending())
}

func TestReadiness_Pending_NilWhenZeroGates(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness()
	require.Nil(t, r.Pending())
}

func TestReadiness_WaitChanClosesWhenAllGatesMark(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("a", "b")
	ch := r.WaitChan()
	select {
	case <-ch:
		require.FailNow(t, "WaitChan should be open before all gates are marked")
	default:
	}
	r.Mark("a")
	select {
	case <-ch:
		require.FailNow(t, "WaitChan should be open with one gate outstanding")
	default:
	}
	r.Mark("b")
	select {
	case <-ch:
	default:
		require.FailNow(t, "WaitChan should be closed once every gate is marked")
	}
}

func TestReadiness_WaitChanClosedImmediatelyWithZeroGates(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness()
	select {
	case <-r.WaitChan():
	default:
		require.FailNow(t, "WaitChan should be closed when there are no gates")
	}
}

func TestReadiness_MarkShuttingDownLatchesReadyFalse(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("a")
	r.Mark("a")
	require.True(t, r.Ready(), "expected ready after mark")
	r.MarkShuttingDown()
	require.False(t, r.Ready(), "MarkShuttingDown must flip Ready to false")
	// Subsequent marks must not flip Ready back to true.
	r.Mark("a")
	require.False(t, r.Ready(), "Ready must stay false after MarkShuttingDown even on further marks")
}

func TestReadiness_ConcurrentMarks(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("a", "b", "c", "d")
	var wg sync.WaitGroup
	for _, g := range []string{"a", "b", "c", "d"} {
		wg.Go(func() {
			for range 100 {
				r.Mark(g)
			}
		})
	}
	wg.Wait()
	require.True(t, r.Ready(), "expected ready after concurrent marks")
}
