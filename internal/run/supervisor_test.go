package run_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"go.uber.org/zap"
)

// recorder is a tiny event log: tests use it to assert ordering of
// start/stop events without relying on wall-clock comparisons.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) record(s string) {
	r.mu.Lock()
	r.events = append(r.events, s)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	copy(out, r.events)
	return out
}

type fakeComponent struct {
	name        string
	rec         *recorder
	startErr    error
	startBlocks bool
	stopErr     error

	startCalled atomic.Int32
	stopCalled  atomic.Int32
}

func (f *fakeComponent) Name() string { return f.name }

func (f *fakeComponent) Start(ctx context.Context) error {
	f.startCalled.Add(1)
	f.rec.record("start:" + f.name)
	if f.startErr != nil {
		return f.startErr
	}
	if !f.startBlocks {
		return nil
	}
	<-ctx.Done()
	return nil
}

func (f *fakeComponent) Stop(_ context.Context) error {
	f.stopCalled.Add(1)
	f.rec.record("stop:" + f.name)
	return f.stopErr
}

func TestSupervisor_GracefulShutdownStopsInReverseOrder(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	a := &fakeComponent{name: "a", rec: rec, startBlocks: true}
	b := &fakeComponent{name: "b", rec: rec, startBlocks: true}
	c := &fakeComponent{name: "c", rec: rec, startBlocks: true}

	sup := run.New(time.Second, zap.NewNop(), a, b, c)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	// Allow goroutines to enter Start.
	waitForCondition(t, func() bool {
		return a.startCalled.Load() == 1 && b.startCalled.Load() == 1 && c.startCalled.Load() == 1
	})

	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	stops := stopOrder(rec.snapshot())
	want := []string{"c", "b", "a"}
	if !slices.Equal(stops, want) {
		t.Fatalf("stop order: want %v, got %v (events: %v)", want, stops, rec.snapshot())
	}
}

func TestSupervisor_FirstFailureCancelsOthersAndReturnsError(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	wantErr := errors.New("a exploded")
	a := &fakeComponent{name: "a", rec: rec, startErr: wantErr}
	b := &fakeComponent{name: "b", rec: rec, startBlocks: true}
	c := &fakeComponent{name: "c", rec: rec, startBlocks: true}

	sup := run.New(time.Second, zap.NewNop(), a, b, c)

	err := sup.Run(t.Context())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run error: want wraps %v, got %v", wantErr, err)
	}

	if a.stopCalled.Load() != 1 || b.stopCalled.Load() != 1 || c.stopCalled.Load() != 1 {
		t.Fatalf("all components must be stopped after a failure; got a=%d b=%d c=%d",
			a.stopCalled.Load(), b.stopCalled.Load(), c.stopCalled.Load())
	}
}

func TestSupervisor_FireAndExitComponentDoesNotEndSupervisor(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	probe := &fakeComponent{name: "probe", rec: rec} // returns nil immediately
	long := &fakeComponent{name: "long", rec: rec, startBlocks: true}

	sup := run.New(time.Second, zap.NewNop(), probe, long)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitForCondition(t, func() bool {
		return probe.startCalled.Load() == 1 && long.startCalled.Load() == 1
	})

	// Probe has already returned; long-runner is still blocking. Supervisor
	// should still be running. Confirm by checking Run hasn't returned.
	select {
	case err := <-done:
		t.Fatalf("Run returned prematurely: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error after cancel: %v", err)
	}
}

func TestSupervisor_PreStopRunsBeforeAnyStop(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	a := &fakeComponent{name: "a", rec: rec, startBlocks: true}

	sup := run.New(time.Second, zap.NewNop(), a)
	sup.OnShutdown(func() { rec.record("preStop") })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool { return a.startCalled.Load() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := rec.snapshot()
	preStopIdx := slices.Index(events, "preStop")
	stopIdx := slices.Index(events, "stop:a")
	if preStopIdx < 0 || stopIdx < 0 {
		t.Fatalf("expected both preStop and stop:a in events: %v", events)
	}
	if preStopIdx >= stopIdx {
		t.Fatalf("preStop must precede stop:a: %v", events)
	}
}

func TestSupervisor_StopErrorsAreJoinedWithRunError(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	runErr := errors.New("a failed during start")
	stopErrA := errors.New("a stop failed")
	stopErrB := errors.New("b stop failed")
	a := &fakeComponent{name: "a", rec: rec, startErr: runErr, stopErr: stopErrA}
	b := &fakeComponent{name: "b", rec: rec, startBlocks: true, stopErr: stopErrB}

	sup := run.New(time.Second, zap.NewNop(), a, b)

	err := sup.Run(t.Context())
	if !errors.Is(err, runErr) {
		t.Errorf("err must wrap runErr: %v", err)
	}
	if !errors.Is(err, stopErrA) {
		t.Errorf("err must wrap stopErrA: %v", err)
	}
	if !errors.Is(err, stopErrB) {
		t.Errorf("err must wrap stopErrB: %v", err)
	}
}

// stopOrder extracts the ordered list of component names whose stop event
// appears in events.
func stopOrder(events []string) []string {
	const prefix = "stop:"
	out := make([]string, 0, len(events))
	for _, e := range events {
		if len(e) > len(prefix) && e[:len(prefix)] == prefix {
			out = append(out, e[len(prefix):])
		}
	}
	return out
}

func waitForCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition never became true within 1s")
}
