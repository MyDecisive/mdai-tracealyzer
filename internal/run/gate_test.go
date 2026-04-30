package run_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/run"
)

type gatedFake struct {
	started atomic.Bool
	stopped atomic.Bool
	block   chan struct{}
}

func (*gatedFake) Name() string { return "fake" }

func (f *gatedFake) Start(ctx context.Context) error {
	f.started.Store(true)
	if f.block == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return nil
	case <-f.block:
		return nil
	}
}

func (f *gatedFake) Stop(_ context.Context) error {
	f.stopped.Store(true)
	return nil
}

func TestGated_DoesNotStartInnerUntilGateCloses(t *testing.T) {
	t.Parallel()

	gate := make(chan struct{})
	inner := &gatedFake{}
	g := run.NewGated(inner, gate)

	done := make(chan error, 1)
	go func() { done <- g.Start(t.Context()) }()

	time.Sleep(20 * time.Millisecond)
	if inner.started.Load() {
		t.Fatal("inner.Start ran before gate closed")
	}

	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Start did not return after gate closed")
	}
	if !inner.started.Load() {
		t.Fatal("inner.Start should have run after gate closed")
	}
}

func TestGated_CtxCancelBeforeGateSkipsInner(t *testing.T) {
	t.Parallel()

	gate := make(chan struct{})
	inner := &gatedFake{block: make(chan struct{})}
	g := run.NewGated(inner, gate)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- g.Start(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return on ctx cancel")
	}
	if inner.started.Load() {
		t.Fatal("inner.Start must not run when ctx cancels before gate")
	}
}

func TestGated_StopForwardsRegardlessOfGateState(t *testing.T) {
	t.Parallel()

	inner := &gatedFake{}
	g := run.NewGated(inner, make(chan struct{}))
	if err := g.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !inner.stopped.Load() {
		t.Fatal("Stop should forward to inner even when gate never fired")
	}
}
