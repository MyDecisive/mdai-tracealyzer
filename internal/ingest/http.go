package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
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
// HTTPServer implements run.Component when constructed with a non-empty
// addr (Start binds and serves). Pre-bound listeners use Serve directly.
type HTTPServer struct {
	server *http.Server
	addr   string
	logger *zap.Logger

	serveDone chan struct{}
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

// Start binds addr and runs Serve until ctx cancels. It returns nil when
// ctx cancels; Stop must be called separately to halt the underlying HTTP
// server.
func (s *HTTPServer) Start(ctx context.Context) error {
	if s.addr == "" {
		return errors.New("HTTPServer.Start: addr is empty")
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	return s.serveListener(ctx, ln)
}

// Serve runs the server on a pre-bound listener. The serve loop ends when
// Shutdown is called; ctx is not consulted.
func (s *HTTPServer) Serve(ln net.Listener) error {
	err := s.server.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *HTTPServer) Stop(ctx context.Context) error { return s.Shutdown(ctx) }

// Shutdown gracefully stops the server. If Start was used, it also waits
// for the inner Serve goroutine to exit.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if s.serveDone != nil {
		<-s.serveDone
	}
	return err
}

func (s *HTTPServer) serveListener(ctx context.Context, ln net.Listener) error {
	s.serveDone = make(chan struct{})
	serveErr := make(chan error, 1)
	go func() {
		defer close(s.serveDone)
		err := s.server.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		return nil
	}
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

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	req := &coltracepb.ExportTraceServiceRequest{}
	if unmarshalErr := proto.Unmarshal(body, req); unmarshalErr != nil {
		http.Error(w, "decode protobuf: "+unmarshalErr.Error(), http.StatusBadRequest)
		return
	}

	rejected, firstErr := record(r.Context(), h.recorder, h.metrics, h.logger, req.GetResourceSpans())
	out, marshalErr := proto.Marshal(buildExportResponse(rejected, firstErr, h.logger))
	if marshalErr != nil {
		h.logger.Error("encode trace response", zap.Error(marshalErr))
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeProtobuf)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}
