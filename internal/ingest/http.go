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

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

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
type HTTPServer struct {
	server *http.Server
	logger *zap.Logger
}

// NewHTTPServer builds the server without starting it; call Serve.
func NewHTTPServer(rec Recorder, metrics *Metrics, logger *zap.Logger) *HTTPServer {
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
		logger: logger,
	}
}

func (s *HTTPServer) Serve(ln net.Listener) error {
	err := s.server.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
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
