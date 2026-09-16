package ingest_test

import (
	"bytes"
	"compress/gzip"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
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

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))
	respBody, _ := io.ReadAll(resp.Body)
	out := &coltracepb.ExportTraceServiceResponse{}
	require.NoError(t, proto.Unmarshal(respBody, out), "decode response")
	assert.Zero(t, out.GetPartialSuccess().GetRejectedSpans(), "rejected_spans")
	assert.Len(t, rec.snapshot(), 1)
	assert.InDelta(t, 1, counterValue(t, reg, "topology_spans_received_total"), 0, "spans_received counter")
}

func TestHTTPServer_RejectsNonPOST(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, http.MethodPost, resp.Header.Get("Allow"))
}

func TestHTTPServer_RejectsNonProtobufContentType(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	resp := doPost(t, url, "application/json", []byte(`{"resourceSpans":[]}`))
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
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
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, rec.snapshot(), 1)
}

func TestHTTPServer_PostTracesAcceptsGzipBody(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	raw := mustMarshal(t, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("checkout", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "POST /orders",
					tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	})
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, err := gz.Write(raw)
	require.NoError(t, err, "gzip write")
	require.NoError(t, gz.Close(), "gzip close")

	resp := doPostWithHeaders(t, url, map[string]string{
		"Content-Type":     "application/x-protobuf",
		"Content-Encoding": "gzip",
	}, compressed.Bytes())
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body = %q", respBody)
	require.Len(t, rec.snapshot(), 1)
}

func TestHTTPServer_RejectsUnsupportedContentEncoding(t *testing.T) {
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

	resp := doPostWithHeaders(t, url, map[string]string{
		"Content-Type":     "application/x-protobuf",
		"Content-Encoding": "deflate",
	}, body)
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode, "body = %q", respBody)
	assert.Empty(t, rec.snapshot())
}

func TestHTTPServer_MalformedGzipReturns400(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	resp := doPostWithHeaders(t, url, map[string]string{
		"Content-Type":     "application/x-protobuf",
		"Content-Encoding": "gzip",
	}, []byte{0x1f, 0x8b, 0x00})
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "body = %q", respBody)
	assert.Empty(t, rec.snapshot())
}

func TestHTTPServer_MalformedProtobufReturns400(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	resp := doPost(t, url, "application/x-protobuf", []byte{0xff, 0xff, 0xff, 0xff})
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
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
	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := &coltracepb.ExportTraceServiceResponse{}
	respBody, _ := io.ReadAll(resp.Body)
	require.NoError(t, proto.Unmarshal(respBody, out), "decode response")
	assert.Equal(t, int64(2), out.GetPartialSuccess().GetRejectedSpans(), "rejected_spans")
	assert.InDelta(t, 3, counterValue(t, reg, "topology_spans_received_total"), 0,
		"spans_received counter counts all decoded spans, incl. rejected")
}

// TestHTTPServer_MixedRejectionsReturnsUnavailable mirrors the gRPC
// policy lock-in: any transient rejection in a batch — even mixed with
// permanent ones — returns 503 so the client retries the whole batch
// and recovers the transient rows.
func TestHTTPServer_MixedRejectionsReturnsUnavailable(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{
		rejectFn: func(r buffer.SpanRecord) error {
			if r.SpanID == rootSpanID {
				return buffer.ErrInvalidSpan
			}
			return buffer.ErrBufferFull
		},
	}
	server := ingest.NewHTTPServer(rec, "", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())
	url, cleanup := startHTTP(t, server)
	t.Cleanup(cleanup)

	body := mustMarshal(t, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("svc", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "r",
					tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
				childSpan(traceIDAllBytes, childSpanID, rootSpanID, "c",
					tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	})
	resp := doPost(t, url, "application/x-protobuf", body)
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "body = %q", respBody)
	assert.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))
	st := &spb.Status{}
	require.NoError(t, proto.Unmarshal(respBody, st), "decode google.rpc.Status body")
	assert.Equal(t, int32(codes.Unavailable), st.GetCode(), "Status.Code")
}

func TestHTTPServer_ReportsClassifiedErrorMessage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		putErr  error
		wantMsg string
	}{
		{"invalid_span", buffer.ErrInvalidSpan, buffer.ErrInvalidSpan.Error()},
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
			require.NoError(t, proto.Unmarshal(respBody, out), "decode response")
			assert.Equal(t, tc.wantMsg, out.GetPartialSuccess().GetErrorMessage(), "ErrorMessage")
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
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.InDelta(t, 2, counterValue(t, reg, "topology_spans_received_total"), 0, "spans_received counter")
	assert.InDelta(t, 1, counterValue(t, reg, "topology_spans_malformed_total", "stage", "ingest"), 0,
		"spans_malformed{stage=ingest} counter")
	assert.Len(t, rec.snapshot(), 1)
}

func TestHTTPServer_StartServesAndShutdownHalts(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	server := ingest.NewHTTPServer(rec, "127.0.0.1:0", ingest.NewMetrics(prometheus.NewRegistry()), zap.NewNop())

	require.NoError(t, server.Start(t.Context(), noopHost{}))

	addr := server.Addr()
	require.NotNil(t, addr, "Addr() returned nil after Start")
	url := "http://" + addr.String() + "/v1/traces"

	body, err := proto.Marshal(&coltracepb.ExportTraceServiceRequest{})
	require.NoError(t, err)
	post := func(ctx context.Context) (*http.Response, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		return http.DefaultClient.Do(req)
	}

	resp, err := post(t.Context())
	require.NoError(t, err, "POST before Shutdown")
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "POST before Shutdown")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	require.NoError(t, server.Shutdown(stopCtx))

	postCtx, postCancel := context.WithTimeout(context.Background(), time.Second)
	defer postCancel()
	resp, err = post(postCtx)
	if err == nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err, "POST succeeded after Shutdown; expected connection failure")
}

// --- helpers ---

func startHTTP(t *testing.T, server *ingest.HTTPServer) (string, func()) {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(ln)
	}()
	url := "http://" + ln.Addr().String() + "/v1/traces"
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		require.NoError(t, <-serveErr, "Serve returned error")
	}
	return url, cleanup
}

func doPost(t *testing.T, url, contentType string, body []byte) *http.Response {
	t.Helper()
	return doPostWithHeaders(t, url, map[string]string{"Content-Type": contentType}, body)
}

func doPostWithHeaders(t *testing.T, url string, headers map[string]string, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()

	b, err := proto.Marshal(m)
	require.NoError(t, err)
	return b
}
