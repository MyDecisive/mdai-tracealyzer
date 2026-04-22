package ingest

import (
	"context"
	"errors"
	"net"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Registers the gzip codec so gRPC accepts gzipped OTLP; OTel SDKs default to gzip.
)

type GRPCServer struct {
	server *grpc.Server
	logger *zap.Logger
}

// NewGRPCServer builds the server without starting it; call Serve.
func NewGRPCServer(rec Recorder, metrics *Metrics, logger *zap.Logger) *GRPCServer {
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(maxRequestBytes))
	coltracepb.RegisterTraceServiceServer(srv, &grpcTraceHandler{
		recorder: rec,
		metrics:  metrics,
		logger:   logger,
	})
	return &GRPCServer{server: srv, logger: logger}
}

func (s *GRPCServer) Serve(ln net.Listener) error {
	err := s.server.Serve(ln)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Shutdown falls back to a hard Stop if ctx expires before GracefulStop
// completes.
func (s *GRPCServer) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		<-done
		return ctx.Err()
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
