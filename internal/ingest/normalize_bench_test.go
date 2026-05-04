package ingest_test

import (
	"fmt"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// BenchmarkNormalize measures the per-call cost of Normalize at varying
// fan-in. The current implementation calls proto.Size(span) per span and
// proto.Size(rs) once per ResourceSpans, so cost should grow ~linearly in
// total span count.
func BenchmarkNormalize(b *testing.B) {
	for _, spansPerRS := range []int{1, 10, 100} {
		for _, numRS := range []int{1, 10} {
			name := fmt.Sprintf("rs=%d/spans_per_rs=%d", numRS, spansPerRS)
			b.Run(name, func(b *testing.B) {
				req := buildRequest(numRS, spansPerRS)
				b.ResetTimer()
				b.ReportAllocs()
				for range b.N {
					_, _ = ingest.Normalize(req)
				}
			})
		}
	}
}

// BenchmarkProtoSizeOnly isolates the marshal-walk cost. Normalize calls
// proto.Size(span) once per span and proto.Size(rs) once per ResourceSpans;
// this benchmark sums both to bound the share of Normalize that is the
// double-walk.
func BenchmarkProtoSizeOnly(b *testing.B) {
	for _, spansPerRS := range []int{1, 10, 100} {
		for _, numRS := range []int{1, 10} {
			name := fmt.Sprintf("rs=%d/spans_per_rs=%d", numRS, spansPerRS)
			b.Run(name, func(b *testing.B) {
				req := buildRequest(numRS, spansPerRS)
				b.ResetTimer()
				for range b.N {
					var sink int
					for _, rs := range req {
						sink += proto.Size(rs)
						for _, ss := range rs.GetScopeSpans() {
							for _, s := range ss.GetSpans() {
								sink += proto.Size(s)
							}
						}
					}
					_ = sink
				}
			})
		}
	}
}

func buildRequest(numRS, spansPerRS int) []*tracepb.ResourceSpans {
	out := make([]*tracepb.ResourceSpans, 0, numRS)
	for r := range numRS {
		spans := make([]*tracepb.Span, 0, spansPerRS)
		root := rootSpan(traceIDAllBytes, rootSpanID, "POST /api/orders",
			tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK,
			strAttr("http.request.method", "POST"),
			strAttr("http.route", "/api/orders"),
		)
		spans = append(spans, root)
		for i := 1; i < spansPerRS; i++ {
			id := [8]byte{0xc0, 0xff, 0xee, byte(r), byte(i >> 8), byte(i), 0, 0}
			child := childSpan(traceIDAllBytes, id, rootSpanID, "SELECT",
				tracepb.Span_SPAN_KIND_CLIENT, tracepb.Status_STATUS_CODE_OK)
			spans = append(spans, child)
		}
		out = append(out, resourceSpans(fmt.Sprintf("svc-%d", r), spans))
	}
	return out
}
