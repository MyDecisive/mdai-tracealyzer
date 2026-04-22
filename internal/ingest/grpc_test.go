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
	server := ingest.NewGRPCServer(rec, metrics, zap.NewNop())

	client, cleanup := dialGRPC(t, server)
	defer cleanup()

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("checkout", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "POST /orders", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
				childSpan(traceIDAllBytes, childSpanID, rootSpanID, "SELECT", tracepb.Span_SPAN_KIND_CLIENT, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	}
	resp, err := client.Export(t.Context(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 0 {
		t.Errorf("rejected_spans = %d, want 0", resp.GetPartialSuccess().GetRejectedSpans())
	}

	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("stored %d records, want 2", len(got))
	}
	if got[0].Service != "checkout" {
		t.Errorf("record[0].Service = %q", got[0].Service)
	}
	if count := counterValue(t, reg, "topology_spans_received_total"); count != 2 {
		t.Errorf("spans_received counter = %v, want 2", count)
	}
}

func TestGRPCServer_Export_AcceptsGzipCompression(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	reg := prometheus.NewRegistry()
	metrics := ingest.NewMetrics(reg)
	server := ingest.NewGRPCServer(rec, metrics, zap.NewNop())

	client, cleanup := dialGRPC(t, server)
	defer cleanup()

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			resourceSpans("gateway-api", []*tracepb.Span{
				rootSpan(traceIDAllBytes, rootSpanID, "GET /checkout", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK),
			}),
		},
	}
	resp, err := client.Export(t.Context(), req, grpc.UseCompressor("gzip"))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 0 {
		t.Errorf("rejected_spans = %d, want 0", resp.GetPartialSuccess().GetRejectedSpans())
	}
	if got := len(rec.snapshot()); got != 1 {
		t.Errorf("stored %d records, want 1", got)
	}
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
	server := ingest.NewGRPCServer(rec, metrics, zap.NewNop())

	client, cleanup := dialGRPC(t, server)
	defer cleanup()

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
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := resp.GetPartialSuccess().GetRejectedSpans(); got != 2 {
		t.Errorf("rejected_spans = %d, want 2", got)
	}
	if count := counterValue(t, reg, "topology_spans_received_total"); count != 3 {
		t.Errorf("spans_received counter = %v, want 3 (all decoded, incl. rejected)", count)
	}
}

func TestGRPCServer_Shutdown_StopsServer(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	metrics := ingest.NewMetrics(prometheus.NewRegistry())
	server := ingest.NewGRPCServer(rec, metrics, zap.NewNop())

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(ln)
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}
}

// --- test helpers ---

//nolint:ireturn // test helper returns the generated interface for brevity.
func dialGRPC(t *testing.T, server *ingest.GRPCServer) (coltracepb.TraceServiceClient, func()) {
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

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
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

	if len(labelKV)%2 != 0 {
		t.Fatalf("counterValue: odd labelKV length %d", len(labelKV))
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !labelsMatch(m.GetLabel(), labelKV) {
				continue
			}
			if m.GetCounter() == nil {
				t.Fatalf("metric %q has no counter value", name)
			}
			return m.GetCounter().GetValue()
		}
		t.Fatalf("metric %q has no sample matching labels %v", name, labelKV)
	}
	t.Fatalf("metric %q not in registry", name)
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
