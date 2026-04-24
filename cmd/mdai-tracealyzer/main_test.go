package main

import (
	"errors"
	"net/http"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/sweep"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
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

	row, orphans, err := topologyComputer{}.Compute(traceID, buffer.TriggerQuiet, records)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if orphans != 0 {
		t.Fatalf("orphans: want 0, got %d", orphans)
	}
	if row.RootService != "checkout" || row.RootOperation != "POST /checkout" {
		t.Fatalf("row root: want checkout::POST /checkout, got %s::%s", row.RootService, row.RootOperation)
	}
	if row.SpanCount != 2 {
		t.Fatalf("span_count: want 2, got %d", row.SpanCount)
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

	row, orphans, err := topologyComputer{}.Compute(traceID, buffer.TriggerMaxTTL, records)
	if !errors.Is(err, sweep.ErrNoRoot) {
		t.Fatalf("err: want ErrNoRoot, got %v", err)
	}
	if orphans != 2 {
		t.Fatalf("orphans: want 2, got %d", orphans)
	}
	if row != (topology.RootMetrics{}) {
		t.Fatalf("row: want zero-value on ErrNoRoot, got %+v", row)
	}
}
