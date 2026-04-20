package common

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	ddtrace "gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Logger struct {
	service string
	base    *log.Logger
}

func NewLogger(service string) *Logger {
	return &Logger{
		service: service,
		base:    log.New(os.Stdout, "", 0),
	}
}

func (l *Logger) Info(ctx context.Context, message string, fields map[string]any) {
	entry := map[string]any{
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"level":       "INFO",
		"message":     message,
		"service":     l.service,
		"dd.trace_id": "0",
		"dd.span_id":  "0",
	}

	traceID, spanID := traceFields(ctx)
	entry["dd.trace_id"] = traceID
	entry["dd.span_id"] = spanID

	for key, value := range fields {
		if value != nil {
			entry[key] = value
		}
	}

	payload, err := json.Marshal(entry)
	if err != nil {
		l.base.Printf(`{"timestamp":"%s","level":"ERROR","service":"%s","message":"failed to marshal log","marshal_error":%q}`, time.Now().UTC().Format(time.RFC3339Nano), l.service, err.Error())
		return
	}
	l.base.Println(string(payload))
}

func traceFields(ctx context.Context) (string, string) {
	if ctx == nil {
		return "0", "0"
	}
	span, ok := ddtracer.SpanFromContext(ctx)
	if !ok || span == nil {
		return "0", "0"
	}
	spanContext := span.Context()
	traceID := fmt.Sprintf("%032x", spanContext.TraceID())
	if w3cContext, ok := spanContext.(ddtrace.SpanContextW3C); ok {
		traceID = w3cContext.TraceID128()
	}
	spanID := fmt.Sprintf("%016x", spanContext.SpanID())
	return traceID, spanID
}
