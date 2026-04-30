package ingest_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	"github.com/prometheus/client_golang/prometheus"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestHTTPServer_PostTraces_StoresRecords(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	reg := prometheus.NewRegistry()
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(reg), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	body := mustMarshal(t, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("checkout", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "POST /orders", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	})
	resp := doPost(t, url, "application/x-protobuf", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", ct)
	}
	respBody, _ := io.ReadAll(resp.Body)
	out := &coltracepb.ExportTraceServiceResponse{}
	if err := proto.Unmarshal(respBody, out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.GetPartialSuccess().GetRejectedSpans() != 0 {
		t.Errorf("rejected_spans = %d, want 0", out.GetPartialSuccess().GetRejectedSpans())
	}
	if got := rec.snapshot(); len(got) != 1 {
		t.Errorf("stored %d records, want 1", len(got))
	}
	if count := counterValue(t, reg, "topology_spans_received_total"); count != 1 {
		t.Errorf("spans_received counter = %v, want 1", count)
	}
}

func TestHTTPServer_RejectsNonPOST(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); allow != http.MethodPost {
		t.Errorf("Allow header = %q, want POST", allow)
	}
}

func TestHTTPServer_RejectsNonProtobufContentType(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	resp := doPost(t, url, "application/json", []byte(`{"resourceSpans":[]}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415", resp.StatusCode)
	}
}

func TestHTTPServer_AcceptsProtobufContentTypeWithParams(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	body := mustMarshal(t, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("svc", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "r", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	})
	resp := doPost(t, url, "application/x-protobuf; charset=utf-8", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if got := len(rec.snapshot()); got != 1 {
		t.Errorf("stored %d records, want 1", got)
	}
}

func TestHTTPServer_MalformedProtobufReturns400(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	resp := doPost(t, url, "application/x-protobuf", []byte{0xff, 0xff, 0xff, 0xff})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTPServer_ReportsRejectedSpans(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{
		rejectFn: func(r buffer.SpanRecord) error {
			if r.ParentSpanID == ([8]byte{}) {
				return nil
			}
			return errors.New("overflow")
		},
	}
	reg := prometheus.NewRegistry()
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(reg), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	body := mustMarshal(t, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("svc", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "r", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
				childSpan(traceIDAllBytes, childSpanID, rootSpanID, "a", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK),
				childSpan(traceIDAllBytes, childSpanID2, rootSpanID, "b", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	})
	resp := doPost(t, url, "application/x-protobuf", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	out := &coltracepb.ExportTraceServiceResponse{}
	respBody, _ := io.ReadAll(resp.Body)
	if err := proto.Unmarshal(respBody, out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := out.GetPartialSuccess().GetRejectedSpans(); got != 2 {
		t.Errorf("rejected_spans = %d, want 2", got)
	}
	if count := counterValue(t, reg, "topology_spans_received_total"); count != 3 {
		t.Errorf("spans_received counter = %v, want 3 (all decoded, incl. rejected)", count)
	}
}

func TestHTTPServer_ReportsClassifiedErrorMessage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		putErr  error
		wantMsg string
	}{
		{"invalid_span", buffer.ErrInvalidSpan, buffer.ErrInvalidSpan.Error()},
		{"buffer_full", buffer.ErrBufferFull, buffer.ErrBufferFull.Error()},
		{"backend_unavailable", buffer.ErrBackendUnavailable, buffer.ErrBackendUnavailable.Error()},
		{"unclassified_fallback", errors.New("some driver-internal thing"), "buffer rejected spans"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &fakeRecorder{rejectFn: func(buffer.SpanRecord) error { return tc.putErr }}
			server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
			url, cleanup := startHTTP(t, server)
			t.Cleanup(cleanup)

			body := mustMarshal(t, &coltracepb.ExportTraceServiceRequest{
				ResourceSpans: []*tracepb.ResourceSpans{
					resourceSpans("svc", []*tracepb.Span{
						rootSpan(traceIDAllBytes, rootSpanID, "r", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
					}),
				},
			})
			resp := doPost(t, url, "application/x-protobuf", body)
			defer func() { _ = resp.Body.Close() }()

			out := &coltracepb.ExportTraceServiceResponse{}
			respBody, _ := io.ReadAll(resp.Body)
			if err := proto.Unmarshal(respBody, out); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got := out.GetPartialSuccess().GetErrorMessage(); got != tc.wantMsg {
				t.Errorf("ErrorMessage = %q, want %q", got, tc.wantMsg)
			}
		})
	}
}

func TestHTTPServer_CountsMalformedSpansAsReceived(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	reg := prometheus.NewRegistry()
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(reg), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	bad := rootSpan([16]byte{}, rootSpanID, "bad", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK)
	good := rootSpan(traceIDAllBytes, childSpanID, "good", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK)
	body := mustMarshal(t, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("svc", []*tracepb.Span{bad, good}),
		},
	})
	resp := doPost(t, url, "application/x-protobuf", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if count := counterValue(t, reg, "topology_spans_received_total"); count != 2 {
		t.Errorf("spans_received counter = %v, want 2", count)
	}
	if count := counterValue(t, reg, "topology_spans_malformed_total", "stage", "ingest"); count != 1 {
		t.Errorf("spans_malformed{stage=ingest} counter = %v, want 1", count)
	}
	if got := len(rec.snapshot()); got != 1 {
		t.Errorf("stored %d records, want 1", got)
	}
}

func TestHTTPServer_StartReturnsOnCtxCancelStopHalts(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "127.0.0.1:0", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())

	ctx, cancel := context.WithCancel(t.Context())
	startErr := make(chan error, 1)
	go func() { startErr <- server.Start(ctx) }()

	// Give Start a moment to bind. We don't need a stronger signal because
	// the test only asserts Start returns on cancel; bind failure would
	// fail the test below regardless.
	time.Sleep(50 * time.Millisecond)

	cancel()
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return on ctx cancel")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := server.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// --- helpers ---

func startHTTP(t *testing.T, server *ingest.HTTPServer) (string, func()) {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(ln)
	}()
	url := "http://" + ln.Addr().String() + "/v1/traces"
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		if err := <-serveErr; err != nil {
			t.Errorf("Serve returned %v", err)
		}
	}
	return url, cleanup
}

func doPost(t *testing.T, url, contentType string, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()

	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return b
}
