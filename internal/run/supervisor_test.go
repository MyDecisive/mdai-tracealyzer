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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.Fail(t, "condition never became true within 1s")
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
	require.Equal(t, []string{"a", "b", "c"}, starts, "start order")
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

	require.ErrorIs(t, err, bootErr, "Run error")
	assert.Equal(t, int32(1), a.startCount.Load(), "a.Start count")
	assert.Equal(t, int32(1), b.startCount.Load(), "b.Start count")
	assert.Equal(t, int32(0), c.startCount.Load(), "c.Start count (b failed before c)")
	assert.Equal(t, int32(1), a.shutdownCount.Load(), "a must be Shutdown after a Start failure")
	assert.Equal(t, int32(1), b.shutdownCount.Load(), "b must be Shutdown after a Start failure")
	assert.Equal(t, int32(1), c.shutdownCount.Load(), "c must be Shutdown after a Start failure")

	enters := eventsByPrefix(rec.snapshot(), "shutdown_enter:")
	require.Equal(t, []string{"c", "b", "a"}, enters, "shutdown order")
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

	require.ErrorIs(t, err, fatalErr, "Run error")
	require.Equal(t, int32(1), a.shutdownCount.Load(), "a must be Shutdown after fatal")
	require.Equal(t, int32(1), b.shutdownCount.Load(), "b must be Shutdown after fatal")
	enters := eventsByPrefix(rec.snapshot(), "shutdown_enter:")
	require.Equal(t, []string{"b", "a"}, enters, "shutdown order")
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
		require.ErrorIs(t, err, fatalErr, "Run error")
	case <-time.After(2 * time.Second):
		require.Fail(t, "Run did not return after Fatal — likely Fatal blocked the goroutine")
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
		require.ErrorIs(t, err, firstFatal, "Run error must wrap firstFatal")
	case <-time.After(2 * time.Second):
		require.Fail(t, "Run did not return — Fatal during shutdown phase likely deadlocked")
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
		require.NoError(t, err, "Run on clean ctx-cancel must return nil")
	case <-time.After(time.Second):
		require.Fail(t, "Run did not return after cancel")
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
	require.Equal(t, want, seq, "shutdown sequence")
}

func TestSupervisor_ShutdownSafeWithoutPriorStart(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	bootErr := errors.New("a refuses")
	a := &fakeComponent{name: "a", rec: rec, startErr: bootErr}
	b := &fakeComponent{name: "b", rec: rec}
	sup := run.New(time.Second, zap.NewNop(), a, b)

	err := sup.Run(t.Context())

	require.ErrorIs(t, err, bootErr, "Run error")
	assert.Equal(t, int32(0), b.startCount.Load(), "b.Start must not run after a fails")
	assert.Equal(t, int32(1), b.shutdownCount.Load(), "b.Shutdown must run despite no prior Start")
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
	err := <-done
	require.NoError(t, err)

	events := rec.snapshot()
	preShutdowns := 0
	for _, e := range events {
		if e == "preShutdown" {
			preShutdowns++
		}
	}
	assert.Equal(t, 1, preShutdowns, "preShutdown hook run count")
	preIdx := slices.Index(events, "preShutdown")
	firstShutdown := -1
	for i, e := range events {
		if strings.HasPrefix(e, "shutdown_enter:") {
			firstShutdown = i
			break
		}
	}
	require.GreaterOrEqual(t, preIdx, 0, "missing preShutdown event: %v", events)
	require.GreaterOrEqual(t, firstShutdown, 0, "missing shutdown_enter event: %v", events)
	require.Less(t, preIdx, firstShutdown, "preShutdown must precede any Shutdown")
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
	err := <-done
	require.NoError(t, err)

	require.True(t, observed, "Shutdown not observed")
	assert.True(t, liveAtCall, "Shutdown ctx was canceled at call time; supervisor must derive a fresh grace ctx via WithoutCancel")
	assert.True(t, hasDeadline, "Shutdown ctx has no deadline; supervisor must apply the grace timeout")
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
	require.ErrorIs(t, err, shutdownErr)
	require.NotErrorIs(t, err, context.Canceled, "clean cancel must not surface context.Canceled")
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
	err := <-done
	require.NoError(t, err, "first Run")

	err = sup.Run(t.Context())
	require.ErrorIs(t, err, run.ErrRunAlreadyCalled, "second Run")
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
	require.ErrorIs(t, err, bootErr)
	require.ErrorIs(t, err, aShutdownErr)
	require.ErrorIs(t, err, bShutdownErr)
}
