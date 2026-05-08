package run_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"go.uber.org/zap"
)

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
	name            string
	rec             *recorder
	startErr        error
	shutdownErr     error
	onStart         func(run.Host)
	onShutdown      func()
	captureShutdown func(context.Context)

	startCount    atomic.Int32
	shutdownCount atomic.Int32
}

func (f *fakeComponent) Name() string { return f.name }

func (f *fakeComponent) Start(_ context.Context, host run.Host) error {
	f.startCount.Add(1)
	f.rec.record("start:" + f.name)
	if f.startErr != nil {
		return f.startErr
	}
	if f.onStart != nil {
		f.onStart(host)
	}
	return nil
}

func (f *fakeComponent) Shutdown(ctx context.Context) error {
	f.shutdownCount.Add(1)
	f.rec.record("shutdown_enter:" + f.name)
	if f.captureShutdown != nil {
		f.captureShutdown(ctx)
	}
	if f.onShutdown != nil {
		f.onShutdown()
	}
	f.rec.record("shutdown_exit:" + f.name)
	return f.shutdownErr
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

func eventsByPrefix(events []string, prefix string) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		if rest, ok := strings.CutPrefix(e, prefix); ok {
			out = append(out, rest)
		}
	}
	return out
}

func TestSupervisor_StartIsSequentialInRegistrationOrder(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	a := &fakeComponent{name: "a", rec: rec}
	b := &fakeComponent{name: "b", rec: rec}
	c := &fakeComponent{name: "c", rec: rec}
	sup := run.New(time.Second, zap.NewNop(), a, b, c)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool {
		return a.startCount.Load() == 1 && b.startCount.Load() == 1 && c.startCount.Load() == 1
	})
	cancel()
	<-done

	starts := eventsByPrefix(rec.snapshot(), "start:")
	if want := []string{"a", "b", "c"}; !slices.Equal(starts, want) {
		t.Fatalf("start order: want %v, got %v", want, starts)
	}
}

func TestSupervisor_StartFailureShutsDownAllRegisteredInReverse(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	bootErr := errors.New("b refuses to start")
	a := &fakeComponent{name: "a", rec: rec}
	b := &fakeComponent{name: "b", rec: rec, startErr: bootErr}
	c := &fakeComponent{name: "c", rec: rec}
	sup := run.New(time.Second, zap.NewNop(), a, b, c)

	err := sup.Run(t.Context())

	if !errors.Is(err, bootErr) {
		t.Fatalf("Run error: want wraps %v, got %v", bootErr, err)
	}
	if got := a.startCount.Load(); got != 1 {
		t.Errorf("a.Start count: want 1, got %d", got)
	}
	if got := b.startCount.Load(); got != 1 {
		t.Errorf("b.Start count: want 1, got %d", got)
	}
	if got := c.startCount.Load(); got != 0 {
		t.Errorf("c.Start count: want 0 (b failed before c), got %d", got)
	}
	if a.shutdownCount.Load() != 1 || b.shutdownCount.Load() != 1 || c.shutdownCount.Load() != 1 {
		t.Errorf("all components must be Shutdown after a Start failure; got a=%d b=%d c=%d",
			a.shutdownCount.Load(), b.shutdownCount.Load(), c.shutdownCount.Load())
	}

	enters := eventsByPrefix(rec.snapshot(), "shutdown_enter:")
	if want := []string{"c", "b", "a"}; !slices.Equal(enters, want) {
		t.Fatalf("shutdown order: want %v, got %v", want, enters)
	}
}

func TestSupervisor_HostFatalTriggersOrderedShutdown(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	fatalErr := errors.New("b exploded")
	a := &fakeComponent{name: "a", rec: rec}
	b := &fakeComponent{name: "b", rec: rec, onStart: func(host run.Host) {
		go host.Fatal("b", fatalErr)
	}}
	sup := run.New(time.Second, zap.NewNop(), a, b)

	err := sup.Run(t.Context())

	if !errors.Is(err, fatalErr) {
		t.Fatalf("Run error: want wraps %v, got %v", fatalErr, err)
	}
	if a.shutdownCount.Load() != 1 || b.shutdownCount.Load() != 1 {
		t.Fatalf("both must be Shutdown after fatal; got a=%d b=%d",
			a.shutdownCount.Load(), b.shutdownCount.Load())
	}
	enters := eventsByPrefix(rec.snapshot(), "shutdown_enter:")
	if want := []string{"b", "a"}; !slices.Equal(enters, want) {
		t.Fatalf("shutdown order: want %v, got %v", want, enters)
	}
}

func TestSupervisor_HostFatalDoesNotBlockCaller(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	fatalErr := errors.New("fail")
	goroutineExited := make(chan struct{})
	fataler := &fakeComponent{
		name: "fataler",
		rec:  rec,
		onStart: func(host run.Host) {
			go func() {
				host.Fatal("fataler", fatalErr)
				close(goroutineExited)
			}()
		},
		// If Fatal blocked the calling goroutine, goroutineExited would
		// never close and Shutdown would deadlock here.
		onShutdown: func() { <-goroutineExited },
	}
	sup := run.New(time.Second, zap.NewNop(), fataler)

	done := make(chan error, 1)
	go func() { done <- sup.Run(t.Context()) }()
	select {
	case err := <-done:
		if !errors.Is(err, fatalErr) {
			t.Fatalf("Run error: want wraps %v, got %v", fatalErr, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Fatal — likely Fatal blocked the goroutine")
	}
}

func TestSupervisor_HostFatalAfterShutdownStartedIsSafe(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	firstFatal := errors.New("first")
	duringShutdownFatal := errors.New("during-shutdown")
	var savedHost run.Host
	fataler := &fakeComponent{
		name: "fataler",
		rec:  rec,
		onStart: func(host run.Host) {
			savedHost = host
			go host.Fatal("fataler", firstFatal)
		},
		// onShutdown runs mid-shutdown; this Fatal must not retrigger
		// shutdown and must not deadlock.
		onShutdown: func() { savedHost.Fatal("fataler", duringShutdownFatal) },
	}
	sup := run.New(time.Second, zap.NewNop(), fataler)

	done := make(chan error, 1)
	go func() { done <- sup.Run(t.Context()) }()
	select {
	case err := <-done:
		if !errors.Is(err, firstFatal) {
			t.Errorf("Run error must wrap firstFatal: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return — Fatal during shutdown phase likely deadlocked")
	}
}

func TestSupervisor_CleanCtxCancelReturnsNil(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	a := &fakeComponent{name: "a", rec: rec}
	b := &fakeComponent{name: "b", rec: rec}
	sup := run.New(time.Second, zap.NewNop(), a, b)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool {
		return a.startCount.Load() == 1 && b.startCount.Load() == 1
	})
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run on clean ctx-cancel must return nil, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestSupervisor_ShutdownIsSequentialAndReverse(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	slow := func() { time.Sleep(5 * time.Millisecond) }
	a := &fakeComponent{name: "a", rec: rec, onShutdown: slow}
	b := &fakeComponent{name: "b", rec: rec, onShutdown: slow}
	c := &fakeComponent{name: "c", rec: rec, onShutdown: slow}
	sup := run.New(time.Second, zap.NewNop(), a, b, c)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool {
		return a.startCount.Load() == 1 && b.startCount.Load() == 1 && c.startCount.Load() == 1
	})
	cancel()
	<-done

	var seq []string
	for _, e := range rec.snapshot() {
		if strings.HasPrefix(e, "shutdown_") {
			seq = append(seq, e)
		}
	}
	want := []string{
		"shutdown_enter:c", "shutdown_exit:c",
		"shutdown_enter:b", "shutdown_exit:b",
		"shutdown_enter:a", "shutdown_exit:a",
	}
	if !slices.Equal(seq, want) {
		t.Fatalf("shutdown sequence: want %v, got %v", want, seq)
	}
}

func TestSupervisor_ShutdownSafeWithoutPriorStart(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	bootErr := errors.New("a refuses")
	a := &fakeComponent{name: "a", rec: rec, startErr: bootErr}
	b := &fakeComponent{name: "b", rec: rec}
	sup := run.New(time.Second, zap.NewNop(), a, b)

	err := sup.Run(t.Context())

	if !errors.Is(err, bootErr) {
		t.Fatalf("Run error: want wraps %v, got %v", bootErr, err)
	}
	if got := b.startCount.Load(); got != 0 {
		t.Errorf("b.Start must not run after a fails: count=%d", got)
	}
	if got := b.shutdownCount.Load(); got != 1 {
		t.Errorf("b.Shutdown must run despite no prior Start: count=%d", got)
	}
}

func TestSupervisor_PreShutdownHookRunsOnceBeforeAnyShutdown(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	a := &fakeComponent{name: "a", rec: rec}
	b := &fakeComponent{name: "b", rec: rec}
	sup := run.New(time.Second, zap.NewNop(), a, b)
	sup.OnShutdown(func() { rec.record("preShutdown") })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool {
		return a.startCount.Load() == 1 && b.startCount.Load() == 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := rec.snapshot()
	preShutdowns := 0
	for _, e := range events {
		if e == "preShutdown" {
			preShutdowns++
		}
	}
	if preShutdowns != 1 {
		t.Errorf("preShutdown hook ran %d times, want 1", preShutdowns)
	}
	preIdx := slices.Index(events, "preShutdown")
	firstShutdown := -1
	for i, e := range events {
		if strings.HasPrefix(e, "shutdown_enter:") {
			firstShutdown = i
			break
		}
	}
	if preIdx < 0 || firstShutdown < 0 {
		t.Fatalf("missing preShutdown or shutdown events: %v", events)
	}
	if preIdx >= firstShutdown {
		t.Fatalf("preShutdown must precede any Shutdown: preIdx=%d firstShutdown=%d", preIdx, firstShutdown)
	}
}

func TestSupervisor_ShutdownReceivesUncanceledGraceCtx(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	var (
		observed    bool
		liveAtCall  bool
		hasDeadline bool
	)
	a := &fakeComponent{
		name: "a", rec: rec,
		captureShutdown: func(ctx context.Context) {
			observed = true
			liveAtCall = ctx.Err() == nil
			_, hasDeadline = ctx.Deadline()
		},
	}
	sup := run.New(time.Second, zap.NewNop(), a)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool { return a.startCount.Load() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !observed {
		t.Fatal("Shutdown not observed")
	}
	if !liveAtCall {
		t.Error("Shutdown ctx was canceled at call time; supervisor must derive a fresh grace ctx via WithoutCancel")
	}
	if !hasDeadline {
		t.Error("Shutdown ctx has no deadline; supervisor must apply the grace timeout")
	}
}

func TestSupervisor_CleanCtxCancelWithShutdownErrorReturnsOnlyShutdownError(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	shutdownErr := errors.New("a shutdown fail")
	a := &fakeComponent{name: "a", rec: rec, shutdownErr: shutdownErr}
	sup := run.New(time.Second, zap.NewNop(), a)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool { return a.startCount.Load() == 1 })
	cancel()

	err := <-done
	if !errors.Is(err, shutdownErr) {
		t.Fatalf("err must wrap shutdownErr: %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("err must not contain context.Canceled (clean cancel): %v", err)
	}
}

func TestSupervisor_RunIsOneShot(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	a := &fakeComponent{name: "a", rec: rec}
	sup := run.New(time.Second, zap.NewNop(), a)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitForCondition(t, func() bool { return a.startCount.Load() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("first Run: %v", err)
	}

	if err := sup.Run(t.Context()); !errors.Is(err, run.ErrRunAlreadyCalled) {
		t.Fatalf("second Run: want ErrRunAlreadyCalled, got %v", err)
	}
}

func TestSupervisor_ShutdownErrorsJoinedWithTrigger(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	bootErr := errors.New("b boot fail")
	aShutdownErr := errors.New("a shutdown fail")
	bShutdownErr := errors.New("b shutdown fail")
	a := &fakeComponent{name: "a", rec: rec, shutdownErr: aShutdownErr}
	b := &fakeComponent{name: "b", rec: rec, startErr: bootErr, shutdownErr: bShutdownErr}
	sup := run.New(time.Second, zap.NewNop(), a, b)

	err := sup.Run(t.Context())
	if !errors.Is(err, bootErr) {
		t.Errorf("err must wrap bootErr: %v", err)
	}
	if !errors.Is(err, aShutdownErr) {
		t.Errorf("err must wrap aShutdownErr: %v", err)
	}
	if !errors.Is(err, bShutdownErr) {
		t.Errorf("err must wrap bShutdownErr: %v", err)
	}
}
