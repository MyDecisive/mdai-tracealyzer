package ingest_test

import (
	"reflect"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

var (
	traceIDAllBytes = [16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	}
	// traceIDDatadog simulates a Datadog-origin trace with the upper 64
	// bits zero (Enable128BitTraceID feature gate disabled upstream).
	traceIDDatadog = [16]byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
	}
	rootSpanID   = [8]byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	childSpanID  = [8]byte{0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8}
	childSpanID2 = [8]byte{0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8}
	otherSpanID  = [8]byte{0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8}
)

func TestNormalize_HappyPath_RootAndChild(t *testing.T) {
	t.Parallel()

	req := []*tracepb.ResourceSpans{
		resourceSpans("checkout", []*tracepb.Span{
			rootSpan(traceIDAllBytes, rootSpanID, "POST /api/orders", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK,
				strAttr("http.request.method", "POST"),
				strAttr("http.route", "/api/orders"),
				strAttr("url.full", "https://example.com/api/orders/123"),
			),
			childSpan(traceIDAllBytes, childSpanID, rootSpanID, "SELECT", tracepb.Span_SPAN_KIND_CLIENT, tracepb.Status_STATUS_CODE_OK),
		}),
	}

	records, _ := ingest.Normalize(req)
	if len(records) != 2 {
		t.Fatalf("want 2 records, got %d", len(records))
	}

	root := records[0]
	if root.TraceID != traceIDAllBytes {
		t.Errorf("root trace_id = %x, want %x", root.TraceID, traceIDAllBytes)
	}
	if root.SpanID != rootSpanID {
		t.Errorf("root span_id = %x, want %x", root.SpanID, rootSpanID)
	}
	if root.ParentSpanID != ([8]byte{}) {
		t.Errorf("root parent_span_id = %x, want zero", root.ParentSpanID)
	}
	if root.Service != "checkout" {
		t.Errorf("root service = %q, want %q", root.Service, "checkout")
	}
	if root.Name != "POST /api/orders" {
		t.Errorf("root name = %q", root.Name)
	}
	if root.Kind != int32(tracepb.Span_SPAN_KIND_SERVER) {
		t.Errorf("root kind = %d", root.Kind)
	}
	if root.StatusError {
		t.Error("root status_error = true, want false")
	}
	wantAttrs := map[string]string{
		"http.request.method": "POST",
		"http.route":          "/api/orders",
	}
	if !reflect.DeepEqual(root.OpAttrs, wantAttrs) {
		t.Errorf("root OpAttrs = %v, want %v", root.OpAttrs, wantAttrs)
	}

	child := records[1]
	if child.ParentSpanID != rootSpanID {
		t.Errorf("child parent_span_id = %x, want %x", child.ParentSpanID, rootSpanID)
	}
	if child.OpAttrs != nil {
		t.Errorf("child OpAttrs = %v, want nil (non-root)", child.OpAttrs)
	}
}

func TestNormalize_MultiResourceSpans_SharesTraceIDAcrossServices(t *testing.T) {
	t.Parallel()

	req := []*tracepb.ResourceSpans{
		resourceSpans("checkout", []*tracepb.Span{
			rootSpan(traceIDAllBytes, rootSpanID, "POST /api/orders", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
		}),
		resourceSpans("inventory", []*tracepb.Span{
			childSpan(traceIDAllBytes, childSpanID, rootSpanID, "reserve", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK),
		}),
	}

	records, _ := ingest.Normalize(req)
	if len(records) != 2 {
		t.Fatalf("want 2 records, got %d", len(records))
	}
	if records[0].Service != "checkout" || records[1].Service != "inventory" {
		t.Errorf("services = %q, %q — want checkout, inventory", records[0].Service, records[1].Service)
	}
	if records[0].TraceID != records[1].TraceID {
		t.Error("records share trace_id: mismatch")
	}
}

func TestNormalize_DatadogFragmentedTraceID_UsedAsIs(t *testing.T) {
	t.Parallel()

	req := []*tracepb.ResourceSpans{
		resourceSpans("checkout", []*tracepb.Span{
			rootSpan(traceIDDatadog, rootSpanID, "GET /", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK,
				// _dd.p.tid must not bleed into the stored trace_id.
				strAttr("_dd.p.tid", "ffffffffffffffff"),
			),
		}),
	}

	records, _ := ingest.Normalize(req)
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if records[0].TraceID != traceIDDatadog {
		t.Errorf("trace_id = %x, want %x", records[0].TraceID, traceIDDatadog)
	}
}

func TestNormalize_RootOperationAttributes_PerProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		attrs []*commonpb.KeyValue
		want  map[string]string
	}{
		{
			name: "http",
			attrs: []*commonpb.KeyValue{
				strAttr("http.request.method", "GET"),
				strAttr("http.route", "/users/:id"),
				strAttr("url.full", "https://x/users/42"),
			},
			want: map[string]string{
				"http.request.method": "GET",
				"http.route":          "/users/:id",
			},
		},
		{
			name: "rpc",
			attrs: []*commonpb.KeyValue{
				strAttr("rpc.service", "catalog.v1.Catalog"),
				strAttr("rpc.method", "GetProduct"),
			},
			want: map[string]string{
				"rpc.service": "catalog.v1.Catalog",
				"rpc.method":  "GetProduct",
			},
		},
		{
			name: "messaging",
			attrs: []*commonpb.KeyValue{
				strAttr("messaging.operation.type", "publish"),
				strAttr("messaging.destination.name", "orders.events"),
			},
			want: map[string]string{
				"messaging.operation.type":   "publish",
				"messaging.destination.name": "orders.events",
			},
		},
		{
			name:  "none",
			attrs: []*commonpb.KeyValue{strAttr("unrelated", "value")},
			want:  nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := []*tracepb.ResourceSpans{
				resourceSpans("svc", []*tracepb.Span{
					rootSpan(traceIDAllBytes, rootSpanID, "root", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, tc.attrs...),
				}),
			}
			records, _ := ingest.Normalize(req)
			if len(records) != 1 {
				t.Fatalf("want 1 record, got %d", len(records))
			}
			if !reflect.DeepEqual(records[0].OpAttrs, tc.want) {
				t.Errorf("OpAttrs = %v, want %v", records[0].OpAttrs, tc.want)
			}
		})
	}
}

func TestNormalize_StatusErrorMappedToBool(t *testing.T) {
	t.Parallel()

	req := []*tracepb.ResourceSpans{
		resourceSpans("svc", []*tracepb.Span{
			rootSpan(traceIDAllBytes, rootSpanID, "root", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_ERROR),
			childSpan(traceIDAllBytes, childSpanID, rootSpanID, "ok", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK),
			childSpan(traceIDAllBytes, childSpanID2, rootSpanID, "err", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_ERROR),
		}),
	}
	records, _ := ingest.Normalize(req)
	if len(records) != 3 {
		t.Fatalf("want 3 records, got %d", len(records))
	}
	if !records[0].StatusError {
		t.Error("root StatusError = false, want true")
	}
	if records[1].StatusError {
		t.Error("ok child StatusError = true, want false")
	}
	if !records[2].StatusError {
		t.Error("err child StatusError = false, want true")
	}
}

func TestNormalize_InvalidIDLengthsAreSkipped(t *testing.T) {
	t.Parallel()

	badTrace := &tracepb.Span{
		TraceId: []byte{0x01, 0x02}, // wrong length
		SpanId:  rootSpanID[:],
		Name:    "bad-trace",
	}
	badSpan := &tracepb.Span{
		TraceId: traceIDAllBytes[:],
		SpanId:  []byte{0x01}, // wrong length
		Name:    "bad-span",
	}
	badParent := &tracepb.Span{
		TraceId:      traceIDAllBytes[:],
		SpanId:       otherSpanID[:],
		ParentSpanId: []byte{0x01, 0x02, 0x03}, // wrong length
		Name:         "bad-parent",
	}
	good := rootSpan(traceIDAllBytes, rootSpanID, "good", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK)

	req := []*tracepb.ResourceSpans{
		resourceSpans("svc", []*tracepb.Span{badTrace, badSpan, badParent, good}),
	}
	records, malformed := ingest.Normalize(req)
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if records[0].Name != "good" {
		t.Errorf("survived record = %q, want good", records[0].Name)
	}
	if malformed != 3 {
		t.Errorf("malformed = %d, want 3", malformed)
	}
}

func TestNormalize_AllZeroIDsAreSkipped(t *testing.T) {
	t.Parallel()

	zeroTrace := [16]byte{}
	zeroSpan := [8]byte{}
	badTrace := rootSpan(zeroTrace, rootSpanID, "bad-trace", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK)
	badSpan := rootSpan(traceIDAllBytes, zeroSpan, "bad-span", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK)
	badParent := childSpan(traceIDAllBytes, childSpanID, rootSpanID, "bad-parent", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK)
	badParent.ParentSpanId = zeroSpan[:]
	good := rootSpan(traceIDAllBytes, rootSpanID, "good", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK)

	req := []*tracepb.ResourceSpans{
		resourceSpans("svc", []*tracepb.Span{badTrace, badSpan, badParent, good}),
	}
	records, malformed := ingest.Normalize(req)
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if records[0].Name != "good" {
		t.Errorf("survived record = %q, want good", records[0].Name)
	}
	if malformed != 3 {
		t.Errorf("malformed = %d, want 3", malformed)
	}
}

func TestNormalize_MissingServiceNameIsEmptyString(t *testing.T) {
	t.Parallel()

	req := []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{}, // no attributes
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "r", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
			},
		}},
	}}
	records, _ := ingest.Normalize(req)
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if records[0].Service != "" {
		t.Errorf("service = %q, want empty", records[0].Service)
	}
}

func TestNormalize_NilEntriesIgnored(t *testing.T) {
	t.Parallel()

	req := []*tracepb.ResourceSpans{
		nil,
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{strAttr("service.name", "svc")},
			},
			ScopeSpans: []*tracepb.ScopeSpans{
				nil,
				{
					Spans: []*tracepb.Span{
						nil,
						rootSpan(traceIDAllBytes, rootSpanID, "r", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
					},
				},
			},
		},
	}
	records, _ := ingest.Normalize(req)
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
}

func TestNormalize_TimestampsPreserved(t *testing.T) {
	t.Parallel()

	const start uint64 = 1_700_000_000_000_000_000
	const end uint64 = 1_700_000_000_123_456_789

	span := rootSpan(traceIDAllBytes, rootSpanID, "r", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK)
	span.StartTimeUnixNano = start
	span.EndTimeUnixNano = end

	req := []*tracepb.ResourceSpans{
		resourceSpans("svc", []*tracepb.Span{span}),
	}
	records, _ := ingest.Normalize(req)
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if records[0].StartTimeNs != int64(start) {
		t.Errorf("start_time_ns = %d, want %d", records[0].StartTimeNs, start)
	}
	if records[0].EndTimeNs != int64(end) {
		t.Errorf("end_time_ns = %d, want %d", records[0].EndTimeNs, end)
	}
}

// --- helpers ---

func resourceSpans(service string, spans []*tracepb.Span) *tracepb.ResourceSpans {
	return &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{strAttr("service.name", service)},
		},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}
}

func rootSpan(
	traceID [16]byte,
	spanID [8]byte,
	name string,
	kind tracepb.Span_SpanKind,
	status tracepb.Status_StatusCode,
	attrs ...*commonpb.KeyValue,
) *tracepb.Span {
	return spanWithParent(traceID, spanID, [8]byte{}, name, kind, status, attrs...)
}

func childSpan(
	traceID [16]byte,
	spanID, parentID [8]byte,
	name string,
	kind tracepb.Span_SpanKind,
	status tracepb.Status_StatusCode,
	attrs ...*commonpb.KeyValue,
) *tracepb.Span {
	return spanWithParent(traceID, spanID, parentID, name, kind, status, attrs...)
}

func spanWithParent(
	traceID [16]byte,
	spanID, parentID [8]byte,
	name string,
	kind tracepb.Span_SpanKind,
	status tracepb.Status_StatusCode,
	attrs ...*commonpb.KeyValue,
) *tracepb.Span {
	s := &tracepb.Span{
		TraceId:    traceID[:],
		SpanId:     spanID[:],
		Name:       name,
		Kind:       kind,
		Attributes: attrs,
		Status:     &tracepb.Status{Code: status},
	}
	if parentID != ([8]byte{}) {
		s.ParentSpanId = parentID[:]
	}
	return s
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}
