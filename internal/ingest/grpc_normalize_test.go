package ingest_test

import (
	"context"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	"github.com/prometheus/client_golang/prometheus"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
)

func TestGRPCServer_MalformedSpansCountTowardRejectedSpans(t *testing.T) {
	rec := &fakeRecorder{}
	reg := prometheus.NewRegistry()
	server := ingest.NewGRPCServer(rec, "", ingest.NewMetrics(reg), zap.NewNop())

	client, cleanup := dialGRPC(t, server)
	t.Cleanup(cleanup)

	// One good span + two malformed spans (bad trace_id length, all-zero
	// span_id). Normalize will drop the malformed; the recorder will
	// receive only the good one.
	good := &tracepb.Span{
		TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Name:    "good",
		Status:  &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
	}
	badTrace := &tracepb.Span{
		TraceId: []byte{1, 2, 3}, // wrong length
		SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Name:    "bad-trace",
	}
	badSpan := &tracepb.Span{
		TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, 0}, // all-zero
		Name:    "bad-span",
	}

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{{
					Key: "service.name",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "svc"},
					},
				}},
			},
			ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{good, badTrace, badSpan}}},
		}},
	}

	resp, err := client.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	got := resp.GetPartialSuccess().GetRejectedSpans()
	if got < 2 {
		t.Fatalf("PartialSuccess.RejectedSpans = %d, want >= 2 (two malformed spans dropped at Normalize)", got)
	}
}
