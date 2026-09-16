package ingest_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeRecorder struct {
	mu      sync.Mutex
	records []buffer.SpanRecord
	// rejectFn, if non-nil, is consulted per record. Returning a non-nil
	// error causes the record to be rejected without being stored.
	rejectFn func(buffer.SpanRecord) error
}

func (f *fakeRecorder) Put(_ context.Context, r buffer.SpanRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.rejectFn != nil {
		if err := f.rejectFn(r); err != nil {
			return err
		}
	}
	f.records = append(f.records, r)
	return nil
}

func (f *fakeRecorder) snapshot() []buffer.SpanRecord {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]buffer.SpanRecord, len(f.records))
	copy(out, f.records)
	return out
}

func TestGRPCServer_Export_StoresNormalizedRecords(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	reg := prometheus.NewRegistry()
	metrics := ingest.NewMetrics(reg)
	server := ingest.NewGRPCServer(rec, "", metrics, zap.NewNop())

	client, cleanup := dialGRPC(t, server)
	t.Cleanup(cleanup)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("checkout", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "POST /orders", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
				childSpan(traceIDAllBytes, childSpanID, rootSpanID, "SELECT", tracepb.Span_SPAN_KIND_CLIENT, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	}
	resp, err := client.Export(t.Context(), req)
	require.NoError(t, err)
	assert.Zero(t, resp.GetPartialSuccess().GetRejectedSpans(), "rejected_spans")

	got := rec.snapshot()
	require.Len(t, got, 2)
	assert.Equal(t, "checkout", got[0].Service)
	assert.InDelta(t, 2, counterValue(t, reg, "topology_spans_received_total"), 0, "spans_received counter")
}

func TestGRPCServer_Export_AcceptsGzipCompression(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	reg := prometheus.NewRegistry()
	metrics := ingest.NewMetrics(reg)
	server := ingest.NewGRPCServer(rec, "", metrics, zap.NewNop())

	client, cleanup := dialGRPC(t, server)
	t.Cleanup(cleanup)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("gateway-api", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "GET /checkout", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	}
	resp, err := client.Export(t.Context(), req, grpc.UseCompressor("gzip"))
	require.NoError(t, err)
	assert.Zero(t, resp.GetPartialSuccess().GetRejectedSpans(), "rejected_spans")
	assert.Len(t, rec.snapshot(), 1)
}

func TestGRPCServer_Export_ReportsRejectedSpans(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{
		rejectFn: func(r buffer.SpanRecord) error {
			if r.ParentSpanID == ([8]byte{}) {
				return nil // accept root
			}
			return errors.New("buffer full")
		},
	}
	reg := prometheus.NewRegistry()
	metrics := ingest.NewMetrics(reg)
	server := ingest.NewGRPCServer(rec, "", metrics, zap.NewNop())

	client, cleanup := dialGRPC(t, server)
	t.Cleanup(cleanup)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("checkout", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "root", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
				childSpan(traceIDAllBytes, childSpanID, rootSpanID, "a", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK),
				childSpan(traceIDAllBytes, childSpanID2, rootSpanID, "b", tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	}
	resp, err := client.Export(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.GetPartialSuccess().GetRejectedSpans(), "rejected_spans")
	assert.InDelta(t, 3, counterValue(t, reg, "topology_spans_received_total"), 0,
		"spans_received counter counts all decoded spans, incl. rejected")
}

func TestGRPCServer_Shutdown_HaltsServer(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	metrics := ingest.NewMetrics(prometheus.NewRegistry())
	server := ingest.NewGRPCServer(rec, "", metrics, zap.NewNop())

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(ln)
	}()

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, server.Shutdown(stopCtx))
	select {
	case err := <-serveErr:
		if err != nil {
			require.ErrorIs(t, err, grpc.ErrServerStopped, "Serve returned unexpected error")
		}
	case <-time.After(2 * time.Second):
		require.Fail(t, "Serve did not return after Shutdown")
	}
}

func TestGRPCServer_StartServesAndShutdownHalts(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	metrics := ingest.NewMetrics(prometheus.NewRegistry())
	server := ingest.NewGRPCServer(rec, "127.0.0.1:0", metrics, zap.NewNop())

	require.NoError(t, server.Start(t.Context(), noopHost{}))

	addr := server.Addr()
	require.NotNil(t, addr, "Addr() returned nil after Start")
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := coltracepb.NewTraceServiceClient(conn)

	_, err = client.Export(t.Context(), &coltracepb.ExportTraceServiceRequest{})
	require.NoError(t, err, "Export before Shutdown")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	require.NoError(t, server.Shutdown(stopCtx))

	postCtx, postCancel := context.WithTimeout(context.Background(), time.Second)
	defer postCancel()
	_, err = client.Export(postCtx, &coltracepb.ExportTraceServiceRequest{})
	require.Error(t, err, "Export succeeded after Shutdown; expected failure")
}

type noopHost struct{}

func (noopHost) Fatal(string, error) {}

// --- test helpers ---

//nolint:ireturn // test helper returns the generated interface for brevity.
func dialGRPC(t *testing.T, server *ingest.GRPCServer) (coltracepb.TraceServiceClient, func()) {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(ln)
	}()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := coltracepb.NewTraceServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-serveErr
	}
	return client, cleanup
}

// counterValue reads a counter by name, optionally filtered by label key/value
// pairs (k1, v1, k2, v2, ...). An unfiltered call requires the family to hold
// exactly one sample. A filtered call picks the first sample whose labels
// match every supplied key/value pair.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) float64 {
	t.Helper()

	require.Zero(t, len(labelKV)%2, "counterValue: odd labelKV length %d", len(labelKV))

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !labelsMatch(m.GetLabel(), labelKV) {
				continue
			}
			require.NotNil(t, m.GetCounter(), "metric %q has no counter value", name)
			return m.GetCounter().GetValue()
		}
		require.Fail(t, "metric has no matching sample", "metric %q has no sample matching labels %v", name, labelKV)
	}
	require.Fail(t, "metric not in registry", "metric %q", name)
	return 0
}

func labelsMatch(labels []*dto.LabelPair, labelKV []string) bool {
	for i := 0; i < len(labelKV); i += 2 {
		k, v := labelKV[i], labelKV[i+1]
		found := false
		for _, l := range labels {
			if l.GetName() == k && l.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
