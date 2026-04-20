package common

import (
	"context"
	"strings"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func StartTracer(service string) func() {
	ddtracer.Start(ddtracer.WithService(service))
	return func() {
		ddtracer.Stop()
	}
}

func AnnotateHTTPSpan(ctx context.Context, method, path, requestID, scenario string) {
	span, ok := ddtracer.SpanFromContext(ctx)
	if !ok || span == nil {
		return
	}
	span.SetTag(ext.ResourceName, method+" "+path)
	span.SetTag(ext.SpanKind, ext.SpanKindServer)
	span.SetTag(ext.SpanType, ext.SpanTypeWeb)
	span.SetTag("http.route", path)
	span.SetTag("request.id", requestID)
	if scenario != "" {
		span.SetTag("scenario", scenario)
	}
}

func AnnotateGRPCSpan(ctx context.Context, fullMethod, requestID, scenario string) {
	span, ok := ddtracer.SpanFromContext(ctx)
	if !ok || span == nil {
		return
	}
	resource := strings.TrimPrefix(fullMethod, "/")
	parts := strings.Split(resource, "/")
	serviceName := resource
	methodName := resource
	if len(parts) == 2 {
		serviceName = parts[0]
		methodName = parts[1]
	}
	span.SetTag(ext.ResourceName, resource)
	span.SetTag(ext.SpanKind, ext.SpanKindServer)
	span.SetTag(ext.SpanType, ext.AppTypeRPC)
	span.SetTag("rpc.system", "grpc")
	span.SetTag("rpc.service", serviceName)
	span.SetTag("rpc.method", methodName)
	span.SetTag("request.id", requestID)
	if scenario != "" {
		span.SetTag("scenario", scenario)
	}
}
