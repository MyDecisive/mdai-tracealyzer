package ingest

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
)

const (
	fallbackRejectMessage    = "buffer rejected spans"
	normalizationDropMessage = "spans dropped at normalization"
)

type Recorder interface {
	Put(ctx context.Context, r buffer.SpanRecord) error
}

type recordOutcome struct {
	rejected  int
	transient error
	permanent error
}

func record(
	ctx context.Context,
	rec Recorder,
	logger *zap.Logger,
	records []buffer.SpanRecord,
) recordOutcome {
	var out recordOutcome
	for _, r := range records {
		logger.Debug("span received",
			zap.String("trace_id", hex.EncodeToString(r.TraceID[:])),
			zap.String("span_id", hex.EncodeToString(r.SpanID[:])),
		)
		if err := rec.Put(ctx, r); err != nil {
			out.rejected++
			if isTransient(err) {
				if out.transient == nil {
					out.transient = err
				}
			} else if out.permanent == nil {
				out.permanent = err
			}
		}
	}
	return out
}

func buildExportResponse(rejected, malformed int, firstErr error, logger *zap.Logger) *coltracepb.ExportTraceServiceResponse {
	resp := &coltracepb.ExportTraceServiceResponse{}
	total := rejected + malformed
	if total == 0 {
		return resp
	}
	msg := messageFor(firstErr, malformed)
	logger.Warn(msg,
		zap.Int("rejected", rejected),
		zap.Int("malformed", malformed),
		zap.Error(firstErr))
	resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
		RejectedSpans: int64(total),
		ErrorMessage:  msg,
	}
	return resp
}

func isTransient(err error) bool {
	return errors.Is(err, buffer.ErrBufferFull) || errors.Is(err, buffer.ErrBackendUnavailable)
}

func classifyForClient(err error) string {
	switch {
	case errors.Is(err, buffer.ErrInvalidSpan):
		return buffer.ErrInvalidSpan.Error()
	case errors.Is(err, buffer.ErrBufferFull):
		return buffer.ErrBufferFull.Error()
	case errors.Is(err, buffer.ErrBackendUnavailable):
		return buffer.ErrBackendUnavailable.Error()
	default:
		return fallbackRejectMessage
	}
}

func messageFor(firstErr error, malformed int) string {
	if firstErr != nil {
		return classifyForClient(firstErr)
	}
	if malformed > 0 {
		return normalizationDropMessage
	}
	return fallbackRejectMessage
}
