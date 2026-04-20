package app_test

import (
	"slices"
	"sync"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/app"
)

func TestReadiness_SingleGate(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http")
	if r.Ready() {
		t.Fatal("expected not ready before Mark")
	}
	r.Mark("http")
	if !r.Ready() {
		t.Fatal("expected ready after Mark")
	}
}

func TestReadiness_MultipleGates(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http", "valkey", "otlp_grpc")
	r.Mark("http")
	r.Mark("valkey")
	if r.Ready() {
		t.Fatal("expected not ready with one gate outstanding")
	}
	r.Mark("otlp_grpc")
	if !r.Ready() {
		t.Fatal("expected ready after all gates marked")
	}
}

func TestReadiness_UnknownGateIsNoop(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http")
	r.Mark("typo")
	if r.Ready() {
		t.Fatal("unknown gate should not flip readiness")
	}
	r.Mark("http")
	if !r.Ready() {
		t.Fatal("expected ready after correct gate")
	}
}

func TestReadiness_IdempotentMark(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("http", "valkey")
	r.Mark("http")
	r.Mark("http")
	r.Mark("http")
	if r.Ready() {
		t.Fatal("Ready should require all gates, not repeated marks")
	}
	r.Mark("valkey")
	if !r.Ready() {
		t.Fatal("expected ready after both unique gates marked")
	}
}

func TestReadiness_ZeroGatesReadyImmediately(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness()
	if !r.Ready() {
		t.Fatal("no gates should mean immediately ready")
	}
}

func TestReadiness_Pending_AllGatesSortedWhenNothingMarked(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("valkey", "otlp_grpc", "otlp_http")
	got := r.Pending()
	want := []string{"otlp_grpc", "otlp_http", "valkey"}
	if !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestReadiness_Pending_ShrinksAsMarksLand(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("a", "b", "c")
	r.Mark("b")
	got := r.Pending()
	want := []string{"a", "c"}
	if !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestReadiness_Pending_NilWhenFullyReady(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness("a")
	r.Mark("a")
	if p := r.Pending(); p != nil {
		t.Fatalf("expected nil, got %v", p)
	}
}

func TestReadiness_Pending_NilWhenZeroGates(t *testing.T) {
	t.Parallel()

	r := app.NewReadiness()
	if p := r.Pending(); p != nil {
		t.Fatalf("expected nil, got %v", p)
	}
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
	if !r.Ready() {
		t.Fatal("expected ready after concurrent marks")
	}
}
