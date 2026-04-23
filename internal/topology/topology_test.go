package topology

import (
	"reflect"
	"testing"
)

func TestComputeSimpleChain(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {
			ParentSpanID: [8]byte{},
			Service:      "checkout",
			Name:         "GET",
			StartTimeNs:  100,
			EndTimeNs:    500,
			OpAttrs: map[string]string{
				attrHTTPRequestMethod: "GET",
				attrHTTPRoute:         "/checkout",
			},
		},
		spanID(2): {
			ParentSpanID: spanID(1),
			Service:      "checkout",
			Name:         "validate-cart",
			StartTimeNs:  120,
			EndTimeNs:    180,
		},
		spanID(3): {
			ParentSpanID: spanID(2),
			Service:      "payment",
			Name:         "Authorize",
			StartTimeNs:  190,
			EndTimeNs:    450,
			StatusError:  true,
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 0 {
		t.Fatalf("want no orphans, got %d", orphans)
	}
	want := []RootMetrics{
		{
			RootID:          "checkout::GET /checkout",
			TraceID:         "0102030405060708090a0b0c0d0e0f10",
			RootService:     "checkout",
			RootOperation:   "GET /checkout",
			Breadth:         1,
			ServiceHopDepth: 2,
			ServiceCount:    2,
			OperationCount:  3,
			SpanCount:       3,
			ErrorCount:      1,
			RootDurationNS:  400,
		},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows mismatch\nwant: %#v\n got: %#v", want, rows)
	}
}

func TestComputeFanoutUsesWidestNonRootSpan(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {
			ParentSpanID: [8]byte{},
			Service:      "api",
			Name:         "root",
			StartTimeNs:  10,
			EndTimeNs:    20,
		},
		spanID(2): {
			ParentSpanID: spanID(1),
			Service:      "api",
			Name:         "fanout",
		},
		spanID(3): {
			ParentSpanID: spanID(2),
			Service:      "db",
			Name:         "query-a",
		},
		spanID(4): {
			ParentSpanID: spanID(2),
			Service:      "cache",
			Name:         "get",
		},
		spanID(5): {
			ParentSpanID: spanID(2),
			Service:      "inventory",
			Name:         "reserve",
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 0 {
		t.Fatalf("want no orphans, got %d", orphans)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].Breadth != 3 {
		t.Fatalf("want breadth 3, got %d", rows[0].Breadth)
	}
	if rows[0].ServiceHopDepth != 2 {
		t.Fatalf("want service hop depth 2, got %d", rows[0].ServiceHopDepth)
	}
	if rows[0].ServiceCount != 4 {
		t.Fatalf("want service count 4, got %d", rows[0].ServiceCount)
	}
}

func TestComputeMissingParentSubtreeIsOrphaned(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {
			ParentSpanID: [8]byte{},
			Service:      "api",
			Name:         "root",
		},
		spanID(2): {
			ParentSpanID: spanID(99),
			Service:      "worker",
			Name:         "missing-parent",
		},
		spanID(3): {
			ParentSpanID: spanID(2),
			Service:      "worker",
			Name:         "descendant",
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 2 {
		t.Fatalf("want 2 orphans, got %d", orphans)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].SpanCount != 1 {
		t.Fatalf("want only root subtree counted, got span_count %d", rows[0].SpanCount)
	}
}

func TestComputeAllOrphansWhenNoAuthenticRoot(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {
			ParentSpanID: spanID(2),
			Service:      "a",
			Name:         "a",
		},
		spanID(2): {
			ParentSpanID: spanID(1),
			Service:      "b",
			Name:         "b",
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if len(rows) != 0 {
		t.Fatalf("want no rows, got %d", len(rows))
	}
	if orphans != 2 {
		t.Fatalf("want 2 orphans, got %d", orphans)
	}
}

func TestComputeToleratesMultipleRoots(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(9): {
			ParentSpanID: [8]byte{},
			Service:      "z",
			Name:         "z-root",
		},
		spanID(1): {
			ParentSpanID: [8]byte{},
			Service:      "a",
			Name:         "a-root",
		},
		spanID(2): {
			ParentSpanID: spanID(1),
			Service:      "a",
			Name:         "child",
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 0 {
		t.Fatalf("want no orphans, got %d", orphans)
	}
	wantRootIDs := []string{"a::a-root", "z::z-root"}
	gotRootIDs := []string{rows[0].RootID, rows[1].RootID}
	if !reflect.DeepEqual(gotRootIDs, wantRootIDs) {
		t.Fatalf("want deterministic root IDs %v, got %v", wantRootIDs, gotRootIDs)
	}
}

func TestComputeOperationDerivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		span Span
		want string
	}{
		{
			name: "http with route",
			span: Span{
				Name: "GET /concrete/123",
				OpAttrs: map[string]string{
					attrHTTPRequestMethod: "GET",
					attrHTTPRoute:         "/orders/{id}",
				},
			},
			want: "GET /orders/{id}",
		},
		{
			name: "http without route",
			span: Span{
				Name: "GET /orders/123",
				OpAttrs: map[string]string{
					attrHTTPRequestMethod: "GET",
				},
			},
			want: "GET /orders/123",
		},
		{
			name: "rpc",
			span: Span{
				Name: "grpc span",
				OpAttrs: map[string]string{
					attrRPCService: "payments.PaymentService",
					attrRPCMethod:  "Authorize",
				},
			},
			want: "payments.PaymentService/Authorize",
		},
		{
			name: "messaging",
			span: Span{
				Name: "kafka span",
				OpAttrs: map[string]string{
					attrMessagingOperationType: "publish",
					attrMessagingDestination:   "orders",
				},
			},
			want: "publish orders",
		},
		{
			name: "unnamed fallback",
			span: Span{},
			want: unnamedOperation,
		},
		{
			name: "http precedence over rpc and messaging",
			span: Span{
				Name: "fallback",
				OpAttrs: map[string]string{
					attrHTTPRequestMethod:      "POST",
					attrHTTPRoute:              "/checkout",
					attrRPCService:             "payments.PaymentService",
					attrRPCMethod:              "Authorize",
					attrMessagingOperationType: "publish",
					attrMessagingDestination:   "orders",
				},
			},
			want: "POST /checkout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := deriveOperation(tt.span); got != tt.want {
				t.Fatalf("want %q, got %q", tt.want, got)
			}
		})
	}
}

func TestComputePreservesNegativeRootDuration(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {
			ParentSpanID: [8]byte{},
			Service:      "api",
			Name:         "root",
			StartTimeNs:  20,
			EndTimeNs:    10,
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 0 {
		t.Fatalf("want no orphans, got %d", orphans)
	}
	if rows[0].RootDurationNS != -10 {
		t.Fatalf("want negative duration preserved, got %d", rows[0].RootDurationNS)
	}
}

func spanID(n byte) [8]byte {
	return [8]byte{n}
}

func traceID() [16]byte {
	return [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}
