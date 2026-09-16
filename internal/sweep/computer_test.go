package sweep_test

import (
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/sweep"
	"github.com/stretchr/testify/require"
)

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

	rows, orphans, err := sweep.TopologyComputer{}.Compute(traceID, records)
	require.NoError(t, err, "Compute")
	require.Zero(t, orphans, "orphans")
	require.Len(t, rows, 1, "rows")
	row := rows[0]
	require.Equal(t, "checkout", row.RootService, "row root service")
	require.Equal(t, "POST /checkout", row.RootOperation, "row root operation")
	require.Equal(t, int32(2), row.SpanCount, "span_count")
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

	rows, orphans, err := sweep.TopologyComputer{}.Compute(traceID, records)
	require.NoError(t, err, "Compute")
	require.Zero(t, orphans, "orphans")
	require.Len(t, rows, 2, "rows")
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.RootService+"::"+r.RootOperation] = true
	}
	require.True(t, seen["svc-a::root-a"] && seen["svc-b::root-b"], "expected both roots in output, got %+v", rows)
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

	rows, orphans, err := sweep.TopologyComputer{}.Compute(traceID, records)
	require.ErrorIs(t, err, sweep.ErrNoRoot)
	require.Equal(t, int32(2), orphans, "orphans")
	require.Empty(t, rows, "rows on ErrNoRoot")
}

func TestTopologyComputer_PropagatesSizeBytesFromRecord(t *testing.T) {
	t.Parallel()

	var traceID [16]byte
	traceID[15] = 4
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
			SizeBytes:   600,
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
			SizeBytes:    150,
		},
	}

	rows, orphans, err := sweep.TopologyComputer{}.Compute(traceID, records)
	require.NoError(t, err, "Compute")
	require.Zero(t, orphans, "orphans")
	require.Len(t, rows, 1, "rows")
	require.Equal(t, int64(750), rows[0].SpanBytesTotal, "SpanBytesTotal (600+150 from SpanRecord.SizeBytes)")
}

func TestTopologyComputer_EmptyInput_IsNotErrNoRoot(t *testing.T) {
	t.Parallel()

	var traceID [16]byte
	rows, orphans, err := sweep.TopologyComputer{}.Compute(traceID, nil)
	require.NoError(t, err, "want nil error on empty input")
	require.Empty(t, rows, "rows")
	require.Zero(t, orphans, "orphans")
}
