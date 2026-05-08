package ingest_test

import (
	"context"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	"github.com/prometheus/client_golang/prometheus"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCServer_BackpressureReturnsUnavailable(t *testing.T) {
	cases := []struct {
		name   string
		putErr error
	}{
		{"buffer_full", buffer.ErrBufferFull},
		{"backend_unavailable", buffer.ErrBackendUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &fakeRecorder{
				rejectFn: func(buffer.SpanRecord) error { return tc.putErr },
			}
			reg := prometheus.NewRegistry()
			metrics := ingest.NewMetrics(reg)
			server := ingest.NewGRPCServer(rec, "", metrics, zap.NewNop())

			client, cleanup := dialGRPC(t, server)
			t.Cleanup(cleanup)

			req := &coltracepb.ExportTraceServiceRequest{
				ResourceSpans: []*tracepb.ResourceSpans{
					resourceSpans("svc", []*tracepb.Span{
						rootSpan(traceIDAllBytes, rootSpanID, "r",
							tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
					}),
				},
			}
			_, err := client.Export(context.Background(), req)
			if err == nil {
				t.Fatalf("Export returned nil error; want %s", codes.Unavailable)
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("not a gRPC status: %v", err)
			}
			if st.Code() != codes.Unavailable {
				t.Fatalf("gRPC code = %s, want %s", st.Code(), codes.Unavailable)
			}
		})
	}
}
