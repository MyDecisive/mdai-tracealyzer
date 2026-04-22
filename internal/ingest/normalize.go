package ingest

import (
	"math"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	serviceNameAttr = string(semconv.ServiceNameKey)

	attrHTTPRequestMethod        = string(semconv.HTTPRequestMethodKey)
	attrHTTPRoute                = string(semconv.HTTPRouteKey)
	attrRPCService               = string(semconv.RPCServiceKey)
	attrRPCMethod                = string(semconv.RPCMethodKey)
	attrMessagingOperationType   = string(semconv.MessagingOperationTypeKey)
	attrMessagingDestinationName = string(semconv.MessagingDestinationNameKey)
)

func isRootOpAttr(key string) bool {
	switch key {
	case attrHTTPRequestMethod,
		attrHTTPRoute,
		attrRPCService,
		attrRPCMethod,
		attrMessagingOperationType,
		attrMessagingDestinationName:
		return true
	}
	return false
}

// Normalize flattens OTLP ResourceSpans into SpanRecord values and also
// returns the number of spans dropped for violating OTLP protocol
// constraints (non-16-byte trace_id, non-8-byte span_id, malformed
// parent_span_id, or timestamps past int64 range). The caller surfaces
// that count via topology_spans_malformed_total{stage="ingest"}; nil
// entries and empty scopes do not contribute.
func Normalize(resourceSpans []*tracepb.ResourceSpans) ([]buffer.SpanRecord, int) {
	records := make([]buffer.SpanRecord, 0, estimateSpanCount(resourceSpans))
	malformed := 0
	for _, rs := range resourceSpans {
		if rs == nil {
			continue
		}
		service := serviceNameFrom(rs.GetResource())
		for _, ss := range rs.GetScopeSpans() {
			if ss == nil {
				continue
			}
			for _, span := range ss.GetSpans() {
				if span == nil {
					continue
				}
				record, ok := toRecord(span, service)
				if !ok {
					malformed++
					continue
				}
				records = append(records, record)
			}
		}
	}
	return records, malformed
}

func estimateSpanCount(resourceSpans []*tracepb.ResourceSpans) int {
	total := 0
	for _, rs := range resourceSpans {
		for _, ss := range rs.GetScopeSpans() {
			total += len(ss.GetSpans())
		}
	}
	return total
}

func toRecord(span *tracepb.Span, service string) (buffer.SpanRecord, bool) {
	traceID, ok := toTraceID(span.GetTraceId())
	if !ok {
		return buffer.SpanRecord{}, false
	}
	spanID, ok := toSpanID(span.GetSpanId())
	if !ok {
		return buffer.SpanRecord{}, false
	}
	parentID, ok := toParentSpanID(span.GetParentSpanId())
	if !ok {
		return buffer.SpanRecord{}, false
	}
	startNs, ok := nanoToInt64(span.GetStartTimeUnixNano())
	if !ok {
		return buffer.SpanRecord{}, false
	}
	endNs, ok := nanoToInt64(span.GetEndTimeUnixNano())
	if !ok {
		return buffer.SpanRecord{}, false
	}

	var opAttrs map[string]string
	if parentID == ([8]byte{}) {
		opAttrs = rootOpAttrs(span.GetAttributes())
	}
	return buffer.SpanRecord{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentID,
		Service:      service,
		Name:         span.GetName(),
		Kind:         int32(span.GetKind()),
		StartTimeNs:  startNs,
		EndTimeNs:    endNs,
		StatusError:  span.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR,
		OpAttrs:      opAttrs,
	}, true
}

// nanoToInt64 rejects values that would overflow int64 (past year 2262).
func nanoToInt64(v uint64) (int64, bool) {
	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}

func toTraceID(raw []byte) ([16]byte, bool) {
	var id [16]byte
	if len(raw) != len(id) || allZero(raw) {
		return id, false
	}
	copy(id[:], raw)
	return id, true
}

func toSpanID(raw []byte) ([8]byte, bool) {
	var id [8]byte
	if len(raw) != len(id) || allZero(raw) {
		return id, false
	}
	copy(id[:], raw)
	return id, true
}

// toParentSpanID accepts empty (root span) or exactly 8 bytes.
func toParentSpanID(raw []byte) ([8]byte, bool) {
	var id [8]byte
	if len(raw) == 0 {
		return id, true
	}
	if len(raw) != len(id) || allZero(raw) {
		return id, false
	}
	copy(id[:], raw)
	return id, true
}

func allZero(raw []byte) bool {
	for _, b := range raw {
		if b != 0 {
			return false
		}
	}
	return true
}

func serviceNameFrom(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, kv := range resource.GetAttributes() {
		if kv.GetKey() == serviceNameAttr {
			return stringValue(kv.GetValue())
		}
	}
	return ""
}

func rootOpAttrs(attrs []*commonpb.KeyValue) map[string]string {
	var out map[string]string
	for _, kv := range attrs {
		key := kv.GetKey()
		if !isRootOpAttr(key) {
			continue
		}
		v := stringValue(kv.GetValue())
		if v == "" {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[key] = v
	}
	return out
}

// stringValue returns "" for non-string AnyValue variants; every attribute
// key consulted here is string-typed per OTel semantic conventions.
func stringValue(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	if sv, ok := v.GetValue().(*commonpb.AnyValue_StringValue); ok {
		return sv.StringValue
	}
	return ""
}
