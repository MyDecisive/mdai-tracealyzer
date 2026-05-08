package ingest

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var _ run.Component = (*HTTPServer)(nil)

const (
	tracesPath          = "/v1/traces"
	contentTypeProtobuf = "application/x-protobuf"
	httpReadHeaderGrace = 5 * time.Second

	// maxRequestBytes caps the inbound OTLP payload on both transports:
	// http.MaxBytesReader for OTLP/HTTP and grpc.MaxRecvMsgSize for OTLP/gRPC.
	maxRequestBytes = 16 * 1024 * 1024
)

// HTTPServer serves OTLP/HTTP at POST /v1/traces. Only
// application/x-protobuf is accepted; application/json is deferred past v1.
//
// Start binds addr and spawns the serve goroutine; tests with pre-bound
// listeners use Serve directly.
type HTTPServer struct {
	server *http.Server
	addr   string
	logger *zap.Logger

	started   bool
	serveDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// NewHTTPServer builds the server without starting it. addr is consumed
// by Start; it may be empty when callers will use Serve(ln) with a
// pre-bound listener.
func NewHTTPServer(rec Recorder, addr string, metrics *Metrics, logger *zap.Logger) *HTTPServer {
	mux := http.NewServeMux()
	mux.Handle(tracesPath, &httpTraceHandler{
		recorder: rec,
		metrics:  metrics,
		logger:   logger,
	})
	return &HTTPServer{
		server: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: httpReadHeaderGrace,
			BaseContext: func(net.Listener) context.Context {
				return context.Background()
			},
		},
		addr:   addr,
		logger: logger,
	}
}

func (*HTTPServer) Name() string { return "otlp_http" }

func (s *HTTPServer) Start(ctx context.Context, host run.Host) error {
	if s.addr == "" {
		return errors.New("HTTPServer.Start: addr is empty")
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.serveDone = make(chan struct{})
	s.started = true
	go func() {
		defer close(s.serveDone)
		err := s.server.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			host.Fatal("otlp_http", err)
		}
	}()
	return nil
}

// Serve runs the server on a pre-bound listener. Returns when the server is
// stopped via Shutdown.
func (s *HTTPServer) Serve(ln net.Listener) error {
	err := s.server.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.closeErr = s.server.Shutdown(ctx)
		if s.started {
			<-s.serveDone
		}
	})
	return s.closeErr
}

func readDecodedBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	var src io.Reader = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	switch enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(src)
		if err != nil {
			http.Error(w, "decode gzip: "+err.Error(), http.StatusBadRequest)
			return nil, false
		}
		defer func() { _ = gz.Close() }()
		src = io.LimitReader(gz, maxRequestBytes+1)
	default:
		http.Error(w, fmt.Sprintf("unsupported content-encoding %q", enc), http.StatusUnsupportedMediaType)
		return nil, false
	}

	body, err := io.ReadAll(src)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return nil, false
	}
	if len(body) > maxRequestBytes {
		http.Error(w, "decoded body exceeds size limit", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	return body, true
}

type httpTraceHandler struct {
	recorder Recorder
	metrics  *Metrics
	logger   *zap.Logger
}

func (h *httpTraceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ct := r.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil || mediaType != contentTypeProtobuf {
		http.Error(w, fmt.Sprintf("unsupported content-type %q; require %s", ct, contentTypeProtobuf), http.StatusUnsupportedMediaType)
		return
	}

	body, ok := readDecodedBody(w, r)
	if !ok {
		return
	}

	req := &coltracepb.ExportTraceServiceRequest{}
	if unmarshalErr := proto.Unmarshal(body, req); unmarshalErr != nil {
		http.Error(w, "decode protobuf: "+unmarshalErr.Error(), http.StatusBadRequest)
		return
	}

	records, malformed := Normalize(req.GetResourceSpans())
	h.metrics.incSpansReceived(len(records) + malformed)
	h.metrics.incSpansMalformed(malformed)
	rejected, firstErr := record(r.Context(), h.recorder, h.logger, records)
	if isTransient(firstErr) {
		h.logger.Warn("ingest: transient backpressure",
			zap.Int("rejected", rejected),
			zap.Error(firstErr))
		http.Error(w, classifyForClient(firstErr), http.StatusServiceUnavailable)
		return
	}
	out, marshalErr := proto.Marshal(buildExportResponse(rejected, malformed, firstErr, h.logger))
	if marshalErr != nil {
		h.logger.Error("encode trace response", zap.Error(marshalErr))
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeProtobuf)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}
