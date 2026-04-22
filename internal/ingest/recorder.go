package ingest

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
)

// fallbackRejectMessage is surfaced when Put returns an error that carries
// no buffer sentinel — most commonly test fakes. Production Put always
// wraps with a sentinel, so this line should not appear on real traffic.
const fallbackRejectMessage = "buffer rejected spans"

// Recorder is the narrow buffer contract ingest depends on. The full
// buffer.Buffer adds Scan/Drain/Close, used only by the sweep loop.
type Recorder interface {
	Put(ctx context.Context, r buffer.SpanRecord) error
}

// record returns the count of spans the recorder rejected and the first
// Put error observed (nil if none). Every decoded span increments
// topology_spans_received_total; buffer rejections are tracked separately
// via topology_buffer_rejected_total.
func record(
	ctx context.Context,
	rec Recorder,
	metrics *Metrics,
	logger *zap.Logger,
	resourceSpans []*tracepb.ResourceSpans,
) (int, error) {
	records, malformed := Normalize(resourceSpans)
	metrics.incSpansReceived(len(records) + malformed)
	metrics.incSpansMalformed(malformed)
	rejected := 0
	var firstErr error
	for _, r := range records {
		logger.Debug("span received",
			zap.String("trace_id", hex.EncodeToString(r.TraceID[:])),
			zap.String("span_id", hex.EncodeToString(r.SpanID[:])),
		)
		if err := rec.Put(ctx, r); err != nil {
			rejected++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return rejected, firstErr
}

func buildExportResponse(rejected int, firstErr error, logger *zap.Logger) *coltracepb.ExportTraceServiceResponse {
	resp := &coltracepb.ExportTraceServiceResponse{}
	if rejected > 0 {
		msg := classifyForClient(firstErr)
		logger.Warn(msg, zap.Int("rejected", rejected), zap.Error(firstErr))
		resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: int64(rejected),
			ErrorMessage:  msg,
		}
	}
	return resp
}

// classifyForClient maps a Put error to a short, stable string suitable for
// the OTLP PartialSuccess.ErrorMessage. The raw error stays in server logs;
// only the classification is returned to clients so driver internals do not
// leak into the protocol surface.
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
