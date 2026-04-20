package common

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/vika/global_ratio_mode/test-stand/demo/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	grpctrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return "json"
}

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

func NewInventoryClient(service, target string) (*grpc.ClientConn, pb.InventoryServiceClient, error) {
	conn, err := grpc.Dial(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(
			grpctrace.UnaryClientInterceptor(
				grpctrace.WithServiceName(service),
				grpctrace.WithSpanOptions(
					ddtracer.Tag(ext.SpanKind, ext.SpanKindClient),
					ddtracer.Tag(ext.SpanType, ext.AppTypeRPC),
				),
			),
		),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewInventoryServiceClient(conn), nil
}

func NewGRPCServer(service string) *grpc.Server {
	return grpc.NewServer(
		grpc.UnaryInterceptor(
			grpctrace.UnaryServerInterceptor(
				grpctrace.WithServiceName(service),
				grpctrace.WithSpanOptions(
					ddtracer.Tag(ext.SpanKind, ext.SpanKindServer),
					ddtracer.Tag(ext.SpanType, ext.AppTypeRPC),
				),
			),
		),
	)
}

func WithRequestMetadata(ctx context.Context, requestID, scenario string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		"x-request-id": requestID,
		"x-scenario":   scenario,
	}))
}

func RequestMetadata(ctx context.Context, fallbackRequestID, fallbackScenario string) (string, string) {
	requestID := fallbackRequestID
	scenario := fallbackScenario
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("x-request-id"); len(values) > 0 && strings.TrimSpace(values[0]) != "" {
			requestID = values[0]
		}
		if values := md.Get("x-scenario"); len(values) > 0 && strings.TrimSpace(values[0]) != "" {
			scenario = values[0]
		}
	}
	if requestID == "" {
		requestID = NewRequestID()
	}
	return requestID, scenario
}
