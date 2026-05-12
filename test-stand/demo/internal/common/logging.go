package common

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	ddtrace "gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Verbosity int

const (
	VerbosityModest Verbosity = iota
	VerbosityHeavy
)

func VerbosityFromEnv() Verbosity {
	switch strings.ToLower(os.Getenv("DEMO_LOG_VERBOSITY")) {
	case "heavy":
		return VerbosityHeavy
	default:
		return VerbosityModest
	}
}

const (
	levelDebug = "DEBUG"
	levelInfo  = "INFO"
	levelWarn  = "WARN"
	levelError = "ERROR"
)

type Logger struct {
	service   string
	base      *log.Logger
	verbosity Verbosity
}

func NewLogger(service string, verbosity Verbosity) *Logger {
	return &Logger{
		service:   service,
		base:      log.New(os.Stdout, "", 0),
		verbosity: verbosity,
	}
}

func (l *Logger) Debug(ctx context.Context, message string, fields map[string]any) {
	if l.verbosity < VerbosityHeavy {
		return
	}
	l.emit(ctx, levelDebug, message, fields)
}

func (l *Logger) Info(ctx context.Context, message string, fields map[string]any) {
	l.emit(ctx, levelInfo, message, fields)
}

func (l *Logger) Warn(ctx context.Context, message string, fields map[string]any) {
	l.emit(ctx, levelWarn, message, fields)
}

func (l *Logger) Error(ctx context.Context, message string, fields map[string]any) {
	l.emit(ctx, levelError, message, fields)
}

func (l *Logger) emit(ctx context.Context, level, message string, fields map[string]any) {
	traceID, spanID := traceFields(ctx)
	entry := map[string]any{
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"level":       level,
		"message":     message,
		"service":     l.service,
		"dd.trace_id": traceID,
		"dd.span_id":  spanID,
	}
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

func StartHeartbeat(logger *Logger) {
	if logger.verbosity < VerbosityHeavy {
		return
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		var seq int64
		for range ticker.C {
			seq++
			logger.Debug(context.Background(), "heartbeat", map[string]any{
				"event": "heartbeat",
				"seq":   seq,
			})
		}
	}()
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
