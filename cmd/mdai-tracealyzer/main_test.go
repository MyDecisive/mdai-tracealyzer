package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/sweep"
	"go.uber.org/zap"
)

func TestNewAdminServer_BaseContextNotCancellable(t *testing.T) {
	t.Parallel()

	srv := newAdminServer(http.NewServeMux())
	if srv.BaseContext == nil {
		t.Fatal("BaseContext is nil")
	}
	if done := srv.BaseContext(nil).Done(); done != nil {
		t.Fatal("BaseContext must not propagate cancellation; got Done() != nil")
	}
}

func TestTopologyComputer_RootPresent_ReturnsRow(t *testing.T) {
	t.Parallel()

	var traceID [16]byte
	traceID[15] = 1
	rootID := [8]byte{1}
	childID := [8]byte{2}
	records := map[string]buffer.SpanRecord{
		"0100000000000000": {
			TraceID:     traceID,
			SpanID:      rootID,
			Service:     "checkout",
			Name:        "POST /checkout",
			StartTimeNs: 100,
			EndTimeNs:   200,
			OpAttrs:     map[string]string{"http.request.method": "POST", "http.route": "/checkout"},
		},
		"0200000000000000": {
			TraceID:      traceID,
			SpanID:       childID,
			ParentSpanID: rootID,
			Service:      "checkout",
			Name:         "db.query",
			StartTimeNs:  110,
			EndTimeNs:    190,
		},
	}

	rows, orphans, err := topologyComputer{}.Compute(traceID, buffer.TriggerQuiet, records)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if orphans != 0 {
		t.Fatalf("orphans: want 0, got %d", orphans)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: want 1, got %d", len(rows))
	}
	row := rows[0]
	if row.RootService != "checkout" || row.RootOperation != "POST /checkout" {
		t.Fatalf("row root: want checkout::POST /checkout, got %s::%s", row.RootService, row.RootOperation)
	}
	if row.SpanCount != 2 {
		t.Fatalf("span_count: want 2, got %d", row.SpanCount)
	}
}

func TestTopologyComputer_MultiRoot_ReturnsAllRows(t *testing.T) {
	t.Parallel()

	var traceID [16]byte
	traceID[15] = 3
	records := map[string]buffer.SpanRecord{
		"0100000000000000": {
			TraceID: traceID,
			SpanID:  [8]byte{1},
			Service: "svc-a",
			Name:    "root-a",
		},
		"0200000000000000": {
			TraceID: traceID,
			SpanID:  [8]byte{2},
			Service: "svc-b",
			Name:    "root-b",
		},
	}

	rows, orphans, err := topologyComputer{}.Compute(traceID, buffer.TriggerQuiet, records)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if orphans != 0 {
		t.Fatalf("orphans: want 0, got %d", orphans)
	}
	if len(rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.RootService+"::"+r.RootOperation] = true
	}
	if !seen["svc-a::root-a"] || !seen["svc-b::root-b"] {
		t.Fatalf("expected both roots in output, got %+v", rows)
	}
}

func TestTopologyComputer_NoRoot_ReturnsErrNoRootAndOrphans(t *testing.T) {
	t.Parallel()

	var traceID [16]byte
	traceID[15] = 2
	// Two children whose declared parents are not present in the buffer.
	records := map[string]buffer.SpanRecord{
		"aa00000000000000": {
			TraceID:      traceID,
			SpanID:       [8]byte{0xaa},
			ParentSpanID: [8]byte{0xff},
			Service:      "svc",
			Name:         "child-a",
		},
		"bb00000000000000": {
			TraceID:      traceID,
			SpanID:       [8]byte{0xbb},
			ParentSpanID: [8]byte{0xff},
			Service:      "svc",
			Name:         "child-b",
		},
	}

	rows, orphans, err := topologyComputer{}.Compute(traceID, buffer.TriggerMaxTTL, records)
	if !errors.Is(err, sweep.ErrNoRoot) {
		t.Fatalf("err: want ErrNoRoot, got %v", err)
	}
	if orphans != 2 {
		t.Fatalf("orphans: want 2, got %d", orphans)
	}
	if len(rows) != 0 {
		t.Fatalf("rows: want empty on ErrNoRoot, got %+v", rows)
	}
}

func TestTopologyComputer_EmptyInput_IsNotErrNoRoot(t *testing.T) {
	t.Parallel()

	var traceID [16]byte
	rows, orphans, err := topologyComputer{}.Compute(traceID, buffer.TriggerQuiet, nil)
	if err != nil {
		t.Fatalf("err: want nil on empty input, got %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows: want empty, got %+v", rows)
	}
	if orphans != 0 {
		t.Fatalf("orphans: want 0, got %d", orphans)
	}
}

func TestWaitForSchemaReady_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	cfg := testEmitterConfig()
	checker := &stubSchemaChecker{}
	sleepCalls := 0

	err := waitForSchemaReady(t.Context(), cfg, zap.NewNop(), checker, func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	})
	if err != nil {
		t.Fatalf("waitForSchemaReady: %v", err)
	}
	if checker.calls != 1 {
		t.Fatalf("calls: want 1, got %d", checker.calls)
	}
	if sleepCalls != 0 {
		t.Fatalf("sleepCalls: want 0, got %d", sleepCalls)
	}
}

func TestWaitForSchemaReady_RetryThenSuccess(t *testing.T) {
	t.Parallel()

	cfg := testEmitterConfig()
	checker := &stubSchemaChecker{
		errs: []error{
			errors.New("not ready"),
			nil,
		},
	}
	var sleeps []time.Duration

	err := waitForSchemaReady(t.Context(), cfg, zap.NewNop(), checker, func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	})
	if err != nil {
		t.Fatalf("waitForSchemaReady: %v", err)
	}
	if checker.calls != 2 {
		t.Fatalf("calls: want 2, got %d", checker.calls)
	}
	if len(sleeps) != 1 {
		t.Fatalf("sleep count: want 1, got %d", len(sleeps))
	}
}

func TestWaitForSchemaReady_ExhaustsRetries(t *testing.T) {
	t.Parallel()

	cfg := testEmitterConfig()
	cfg.MaxRetries = 2
	wantErr := errors.New("still not ready")
	checker := &stubSchemaChecker{
		errs: []error{wantErr, wantErr, wantErr},
	}
	var sleeps []time.Duration

	err := waitForSchemaReady(t.Context(), cfg, zap.NewNop(), checker, func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err: want %v, got %v", wantErr, err)
	}
	if checker.calls != 3 {
		t.Fatalf("calls: want 3, got %d", checker.calls)
	}
	if len(sleeps) != 2 {
		t.Fatalf("sleep count: want 2, got %d", len(sleeps))
	}
	if sleeps[0] != cfg.InitialBackoff.Duration() {
		t.Fatalf("sleep[0]: want %v, got %v", cfg.InitialBackoff.Duration(), sleeps[0])
	}
	if sleeps[1] != 2*cfg.InitialBackoff.Duration() {
		t.Fatalf("sleep[1]: want %v, got %v", 2*cfg.InitialBackoff.Duration(), sleeps[1])
	}
}

func TestWaitForSchemaReady_CancelledDuringBackoff(t *testing.T) {
	t.Parallel()

	cfg := testEmitterConfig()
	checker := &stubSchemaChecker{
		errs: []error{errors.New("not ready")},
	}
	ctx, cancel := context.WithCancel(t.Context())

	err := waitForSchemaReady(ctx, cfg, zap.NewNop(), checker, func(context.Context, time.Duration) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err: want context canceled, got %v", err)
	}
	if checker.calls != 1 {
		t.Fatalf("calls: want 1, got %d", checker.calls)
	}
}

type stubSchemaChecker struct {
	calls int
	errs  []error
}

func (s *stubSchemaChecker) CheckReady(context.Context) error {
	s.calls++
	if len(s.errs) == 0 {
		return nil
	}
	idx := s.calls - 1
	if idx >= len(s.errs) {
		return s.errs[len(s.errs)-1]
	}
	return s.errs[idx]
}

func testEmitterConfig() config.Emitter {
	return config.Emitter{
		GreptimeDBSqlEndpoint: "greptimedb:4003",
		GreptimeDBDatabase:    "public",
		MaxRetries:            3,
		InitialBackoff:        config.Duration(100 * time.Millisecond),
	}
}
