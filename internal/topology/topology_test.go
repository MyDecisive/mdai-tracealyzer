package topology

import (
	"math"
	"reflect"
	"testing"
)

func TestCompute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		spans       map[[8]byte]Span
		wantRows    []RootMetrics
		wantOrphans int32
	}{
		{
			name:        "empty input",
			spans:       map[[8]byte]Span{},
			wantRows:    nil,
			wantOrphans: 0,
		},
		{
			name: "single span trace",
			spans: map[[8]byte]Span{
				spanID(1): {
					ParentSpanID: [8]byte{},
					Service:      "checkout",
					Name:         "root",
					StartTimeNs:  100,
					EndTimeNs:    180,
				},
			},
			wantRows: []RootMetrics{
				{
					RootID:          "checkout::root",
					TraceID:         traceIDHex,
					RootService:     "checkout",
					RootOperation:   "root",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  80,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "simple chain",
			spans: map[[8]byte]Span{
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
			},
			wantRows: []RootMetrics{
				{
					RootID:          "checkout::GET /checkout",
					TraceID:         traceIDHex,
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
			},
			wantOrphans: 0,
		},
		{
			name: "basic fanout with widest non root span",
			spans: map[[8]byte]Span{
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
			},
			wantRows: []RootMetrics{
				{
					RootID:          "api::root",
					TraceID:         traceIDHex,
					RootService:     "api",
					RootOperation:   "root",
					Breadth:         3,
					ServiceHopDepth: 2,
					ServiceCount:    4,
					OperationCount:  5,
					SpanCount:       5,
					ErrorCount:      0,
					RootDurationNS:  10,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "rollback saga three way fanout",
			spans: map[[8]byte]Span{
				spanID(1): {
					ParentSpanID: [8]byte{},
					Service:      "checkout",
					Name:         "POST /checkout",
					StartTimeNs:  100,
					EndTimeNs:    900,
					OpAttrs: map[string]string{
						attrHTTPRequestMethod: "POST",
						attrHTTPRoute:         "/checkout",
					},
				},
				spanID(2): {
					ParentSpanID: spanID(1),
					Service:      "inventory",
					Name:         "reserve",
					StartTimeNs:  120,
					EndTimeNs:    260,
				},
				spanID(3): {
					ParentSpanID: spanID(1),
					Service:      "payment",
					Name:         "authorize",
					StartTimeNs:  130,
					EndTimeNs:    320,
					StatusError:  true,
				},
				spanID(4): {
					ParentSpanID: spanID(1),
					Service:      "shipping",
					Name:         "create-label",
					StartTimeNs:  140,
					EndTimeNs:    330,
				},
				spanID(5): {
					ParentSpanID: spanID(3),
					Service:      "payment",
					Name:         "rollback-authorize",
					StartTimeNs:  340,
					EndTimeNs:    420,
					StatusError:  true,
				},
				spanID(6): {
					ParentSpanID: spanID(2),
					Service:      "inventory",
					Name:         "release",
					StartTimeNs:  350,
					EndTimeNs:    430,
				},
				spanID(7): {
					ParentSpanID: spanID(4),
					Service:      "shipping",
					Name:         "cancel-label",
					StartTimeNs:  360,
					EndTimeNs:    440,
				},
			},
			wantRows: []RootMetrics{
				{
					RootID:          "checkout::POST /checkout",
					TraceID:         traceIDHex,
					RootService:     "checkout",
					RootOperation:   "POST /checkout",
					Breadth:         3,
					ServiceHopDepth: 2,
					ServiceCount:    4,
					OperationCount:  7,
					SpanCount:       7,
					ErrorCount:      2,
					RootDurationNS:  800,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "missing parent subtree is orphaned",
			spans: map[[8]byte]Span{
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
			},
			wantRows: []RootMetrics{
				{
					RootID:          "api::root",
					TraceID:         traceIDHex,
					RootService:     "api",
					RootOperation:   "root",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  0,
				},
			},
			wantOrphans: 2,
		},
		{
			name: "all orphan trace",
			spans: map[[8]byte]Span{
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
			},
			wantRows:    []RootMetrics{},
			wantOrphans: 2,
		},
		{
			name: "multi root trace",
			spans: map[[8]byte]Span{
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
			},
			wantRows: []RootMetrics{
				{
					RootID:          "a::a-root",
					TraceID:         traceIDHex,
					RootService:     "a",
					RootOperation:   "a-root",
					Breadth:         1,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  2,
					SpanCount:       2,
					ErrorCount:      0,
					RootDurationNS:  0,
				},
				{
					RootID:          "z::z-root",
					TraceID:         traceIDHex,
					RootService:     "z",
					RootOperation:   "z-root",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  0,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "unnamed root span",
			spans: map[[8]byte]Span{
				spanID(1): {
					ParentSpanID: [8]byte{},
					Service:      "api",
					Name:         "",
				},
			},
			wantRows: []RootMetrics{
				{
					RootID:          "api::unnamed_operation",
					TraceID:         traceIDHex,
					RootService:     "api",
					RootOperation:   "unnamed_operation",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  0,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "http root without route",
			spans: map[[8]byte]Span{
				spanID(1): {
					ParentSpanID: [8]byte{},
					Service:      "api",
					Name:         "GET /orders/123",
					OpAttrs: map[string]string{
						attrHTTPRequestMethod: "GET",
					},
				},
			},
			wantRows: []RootMetrics{
				{
					RootID:          "api::GET /orders/123",
					TraceID:         traceIDHex,
					RootService:     "api",
					RootOperation:   "GET /orders/123",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  0,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "rpc root",
			spans: map[[8]byte]Span{
				spanID(1): {
					ParentSpanID: [8]byte{},
					Service:      "api",
					Name:         "grpc span",
					OpAttrs: map[string]string{
						attrRPCService: "payments.PaymentService",
						attrRPCMethod:  "Authorize",
					},
				},
			},
			wantRows: []RootMetrics{
				{
					RootID:          "api::payments.PaymentService/Authorize",
					TraceID:         traceIDHex,
					RootService:     "api",
					RootOperation:   "payments.PaymentService/Authorize",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  0,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "messaging root",
			spans: map[[8]byte]Span{
				spanID(1): {
					ParentSpanID: [8]byte{},
					Service:      "api",
					Name:         "kafka span",
					OpAttrs: map[string]string{
						attrMessagingOperationType: "publish",
						attrMessagingDestination:   "orders",
					},
				},
			},
			wantRows: []RootMetrics{
				{
					RootID:          "api::publish orders",
					TraceID:         traceIDHex,
					RootService:     "api",
					RootOperation:   "publish orders",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  0,
				},
			},
			wantOrphans: 0,
		},
		{
			name: "negative root duration",
			spans: map[[8]byte]Span{
				spanID(1): {
					ParentSpanID: [8]byte{},
					Service:      "api",
					Name:         "root",
					StartTimeNs:  20,
					EndTimeNs:    10,
				},
			},
			wantRows: []RootMetrics{
				{
					RootID:          "api::root",
					TraceID:         traceIDHex,
					RootService:     "api",
					RootOperation:   "root",
					Breadth:         0,
					ServiceHopDepth: 1,
					ServiceCount:    1,
					OperationCount:  1,
					SpanCount:       1,
					ErrorCount:      0,
					RootDurationNS:  -10,
				},
			},
			wantOrphans: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, orphans := Compute(traceID(), tt.spans)

			if orphans != tt.wantOrphans {
				t.Fatalf("orphans mismatch: want %d, got %d", tt.wantOrphans, orphans)
			}
			if !reflect.DeepEqual(rows, tt.wantRows) {
				t.Fatalf("rows mismatch\nwant: %#v\n got: %#v", tt.wantRows, rows)
			}
		})
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

func TestComputeNilInputReturnsNilRows(t *testing.T) {
	t.Parallel()

	rows, orphans := Compute(traceID(), nil)

	if rows != nil {
		t.Fatalf("want nil rows, got %#v", rows)
	}
	if orphans != 0 {
		t.Fatalf("want 0 orphans, got %d", orphans)
	}
}

func TestComputeBranchingServiceHopDepth(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {ParentSpanID: [8]byte{}, Service: "svc1", Name: "root"},
		spanID(2): {ParentSpanID: spanID(1), Service: "svc1", Name: "same-service"},
		spanID(3): {ParentSpanID: spanID(2), Service: "svc2", Name: "cross-1"},
		spanID(4): {ParentSpanID: spanID(3), Service: "svc1", Name: "cross-2"},
		spanID(5): {ParentSpanID: spanID(4), Service: "svc2", Name: "cross-3"},
		spanID(6): {ParentSpanID: spanID(1), Service: "svc1", Name: "sibling"},
		spanID(7): {ParentSpanID: spanID(6), Service: "svc1", Name: "sibling-leaf"},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 0 {
		t.Fatalf("want 0 orphans, got %d", orphans)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].ServiceHopDepth != 4 {
		t.Fatalf("want service hop depth 4, got %d", rows[0].ServiceHopDepth)
	}
}

func TestComputeOperationCountUsesServiceAndOperationPair(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {ParentSpanID: [8]byte{}, Service: "svc-a", Name: "root"},
		spanID(2): {ParentSpanID: spanID(1), Service: "svc-a", Name: "shared-op"},
		spanID(3): {ParentSpanID: spanID(1), Service: "svc-a", Name: "shared-op"},
		spanID(4): {ParentSpanID: spanID(1), Service: "svc-b", Name: "shared-op"},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 0 {
		t.Fatalf("want 0 orphans, got %d", orphans)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].OperationCount != 3 {
		t.Fatalf("want operation count 3, got %d", rows[0].OperationCount)
	}
}

func TestComputeErrorCountExcludesOrphanedErrors(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {ParentSpanID: [8]byte{}, Service: "api", Name: "root", StatusError: true},
		spanID(2): {ParentSpanID: spanID(1), Service: "api", Name: "mid", StatusError: true},
		spanID(3): {ParentSpanID: spanID(2), Service: "api", Name: "leaf", StatusError: true},
		spanID(4): {ParentSpanID: spanID(99), Service: "worker", Name: "orphan-root", StatusError: true},
		spanID(5): {ParentSpanID: spanID(4), Service: "worker", Name: "orphan-leaf", StatusError: true},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 2 {
		t.Fatalf("want 2 orphans, got %d", orphans)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].ErrorCount != 3 {
		t.Fatalf("want error count 3, got %d", rows[0].ErrorCount)
	}
}

func TestComputeTraceIDHexEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		traceID [16]byte
		want    string
	}{
		{
			name:    "zero padded lowercase",
			traceID: [16]byte{15: 1},
			want:    "00000000000000000000000000000001",
		},
		{
			name:    "high bytes lowercase",
			traceID: [16]byte{0xff, 0xa0, 0x0b, 0x01},
			want:    "ffa00b01000000000000000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, orphans := Compute(tt.traceID, map[[8]byte]Span{
				spanID(1): {ParentSpanID: [8]byte{}, Service: "svc", Name: "root"},
			})

			if orphans != 0 {
				t.Fatalf("want 0 orphans, got %d", orphans)
			}
			if len(rows) != 1 {
				t.Fatalf("want 1 row, got %d", len(rows))
			}
			if rows[0].TraceID != tt.want {
				t.Fatalf("want trace id %q, got %q", tt.want, rows[0].TraceID)
			}
		})
	}
}

func TestComputeRootOperationUsesOnlyRootAttrs(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {ParentSpanID: [8]byte{}, Service: "api", Name: "root-name"},
		spanID(2): {
			ParentSpanID: spanID(1),
			Service:      "worker",
			Name:         "child-name",
			OpAttrs: map[string]string{
				attrHTTPRequestMethod: "GET",
				attrHTTPRoute:         "/child",
			},
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if orphans != 0 {
		t.Fatalf("want 0 orphans, got %d", orphans)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].RootOperation != "root-name" {
		t.Fatalf("want root operation %q, got %q", "root-name", rows[0].RootOperation)
	}
}

func TestComputeSelfParentedSpanBecomesOrphan(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(1): {
			ParentSpanID: spanID(1),
			Service:      "svc",
			Name:         "self",
		},
	}

	rows, orphans := Compute(traceID(), spans)

	if len(rows) != 0 {
		t.Fatalf("want no rows, got %#v", rows)
	}
	if orphans != 1 {
		t.Fatalf("want 1 orphan, got %d", orphans)
	}
}

func TestComputeDeterministicOutput(t *testing.T) {
	t.Parallel()

	spans := map[[8]byte]Span{
		spanID(9): {ParentSpanID: [8]byte{}, Service: "z", Name: "z-root"},
		spanID(1): {ParentSpanID: [8]byte{}, Service: "a", Name: "a-root"},
		spanID(2): {ParentSpanID: spanID(1), Service: "b", Name: "child-b"},
		spanID(3): {ParentSpanID: spanID(1), Service: "a", Name: "child-a"},
	}

	wantRows, wantOrphans := Compute(traceID(), spans)
	for range 20 {
		gotRows, gotOrphans := Compute(traceID(), spans)
		if gotOrphans != wantOrphans {
			t.Fatalf("want orphan count %d, got %d", wantOrphans, gotOrphans)
		}
		if !reflect.DeepEqual(gotRows, wantRows) {
			t.Fatalf("non-deterministic rows\nwant: %#v\n got: %#v", wantRows, gotRows)
		}
	}
}

func TestSafeInt32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int32
	}{
		{name: "negative clamps to zero", in: -1, want: 0},
		{name: "zero stays zero", in: 0, want: 0},
		{name: "positive value passes through", in: 7, want: 7},
		{name: "max int32 passes through", in: math.MaxInt32, want: math.MaxInt32},
		{name: "overflow clamps", in: math.MaxInt32 + 1, want: math.MaxInt32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := safeInt32(tt.in); got != tt.want {
				t.Fatalf("want %d, got %d", tt.want, got)
			}
		})
	}
}

const traceIDHex = "0102030405060708090a0b0c0d0e0f10"

func spanID(n byte) [8]byte {
	return [8]byte{n}
}

func traceID() [16]byte {
	return [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}
