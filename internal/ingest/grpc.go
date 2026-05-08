package ingest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // Registers the gzip codec so gRPC accepts gzipped OTLP; OTel SDKs default to gzip.
	"google.golang.org/grpc/status"
)

var _ run.Component = (*GRPCServer)(nil)

// GRPCServer is the OTLP/gRPC ingest endpoint. Start binds addr and spawns
// the serve goroutine. Tests with pre-bound listeners use Serve directly.
type GRPCServer struct {
	server *grpc.Server
	addr   string
	logger *zap.Logger

	started   bool
	serveDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func NewGRPCServer(rec Recorder, addr string, metrics *Metrics, logger *zap.Logger) *GRPCServer {
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(maxRequestBytes))
	coltracepb.RegisterTraceServiceServer(srv, &grpcTraceHandler{
		recorder: rec,
		metrics:  metrics,
		logger:   logger,
	})
	return &GRPCServer{server: srv, addr: addr, logger: logger}
}

func (*GRPCServer) Name() string { return "otlp_grpc" }

func (s *GRPCServer) Start(ctx context.Context, host run.Host) error {
	if s.addr == "" {
		return errors.New("GRPCServer.Start: addr is empty")
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
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			host.Fatal("otlp_grpc", err)
		}
	}()
	return nil
}

// Serve runs the server on a pre-bound listener. Returns when the server is
// stopped via Shutdown.
func (s *GRPCServer) Serve(ln net.Listener) error {
	err := s.server.Serve(ln)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func (s *GRPCServer) Shutdown(ctx context.Context) error {
	s.closeOnce.Do(func() {
		done := make(chan struct{})
		go func() {
			s.server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.server.Stop()
			<-done
			s.closeErr = ctx.Err()
		}
		if s.started {
			<-s.serveDone
		}
	})
	return s.closeErr
}

type grpcTraceHandler struct {
	coltracepb.UnimplementedTraceServiceServer

	recorder Recorder
	metrics  *Metrics
	logger   *zap.Logger
}

func (h *grpcTraceHandler) Export(
	ctx context.Context,
	req *coltracepb.ExportTraceServiceRequest,
) (*coltracepb.ExportTraceServiceResponse, error) {
	records, malformed := Normalize(req.GetResourceSpans())
	h.metrics.incSpansReceived(len(records) + malformed)
	h.metrics.incSpansMalformed(malformed)
	rejected, firstErr := record(ctx, h.recorder, h.logger, records)
	if isTransient(firstErr) {
		h.logger.Warn("ingest: transient backpressure",
			zap.Int("rejected", rejected),
			zap.Error(firstErr))
		return nil, status.Error(codes.Unavailable, classifyForClient(firstErr))
	}
	return buildExportResponse(rejected, malformed, firstErr, h.logger), nil
}
