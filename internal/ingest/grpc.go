package ingest

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Registers the gzip codec so gRPC accepts gzipped OTLP; OTel SDKs default to gzip.
)

var _ run.Component = (*GRPCServer)(nil)

// GRPCServer is the OTLP/gRPC ingest endpoint. It implements run.Component
// when constructed with a non-empty addr (Start binds and serves). For
// pre-bound listeners (used by tests), call Serve directly.
type GRPCServer struct {
	server *grpc.Server
	addr   string
	logger *zap.Logger

	serveDone chan struct{}
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

// Start binds addr and runs Serve until ctx cancels. It returns nil when
// ctx cancels (Stop must be called separately to actually halt the
// underlying gRPC server) and the wrapped error if Serve fails for any
// reason other than a graceful stop.
func (s *GRPCServer) Start(ctx context.Context) error {
	if s.addr == "" {
		return errors.New("GRPCServer.Start: addr is empty")
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
func (s *GRPCServer) Serve(ln net.Listener) error {
	err := s.server.Serve(ln)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func (s *GRPCServer) Stop(ctx context.Context) error { return s.Shutdown(ctx) }

// Shutdown gracefully stops the server. If ctx expires before GracefulStop
// finishes, it falls back to a hard Stop.
func (s *GRPCServer) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		s.waitForServeExit()
		return nil
	case <-ctx.Done():
		s.server.Stop()
		<-done
		s.waitForServeExit()
		return ctx.Err()
	}
}

func (s *GRPCServer) serveListener(ctx context.Context, ln net.Listener) error {
	s.serveDone = make(chan struct{})
	serveErr := make(chan error, 1)
	go func() {
		defer close(s.serveDone)
		err := s.server.Serve(ln)
		if errors.Is(err, grpc.ErrServerStopped) {
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

func (s *GRPCServer) waitForServeExit() {
	if s.serveDone != nil {
		<-s.serveDone
	}
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
	rejected, firstErr := record(ctx, h.recorder, h.metrics, h.logger, req.GetResourceSpans())
	return buildExportResponse(rejected, firstErr, h.logger), nil
}
