package buffer

import (
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
)

// spanRecordWire is the buffer's on-disk encoding of SpanRecord; trace and
// span IDs are hex strings.
type spanRecordWire struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id"`
	Service      string            `json:"service"`
	Name         string            `json:"name"`
	Kind         int32             `json:"kind"`
	StartTimeNs  int64             `json:"start_time_ns"`
	EndTimeNs    int64             `json:"end_time_ns"`
	StatusError  bool              `json:"status_error"`
	SizeBytes    int64             `json:"size_bytes"`
	OpAttrs      map[string]string `json:"op_attrs,omitempty"`
}

func encodeRecord(r SpanRecord) ([]byte, error) {
	data, err := json.Marshal(spanRecordWire{
		TraceID:      traceIDHex(r.TraceID),
		SpanID:       spanIDHex(r.SpanID),
		ParentSpanID: spanIDHex(r.ParentSpanID),
		Service:      r.Service,
		Name:         r.Name,
		Kind:         r.Kind,
		StartTimeNs:  r.StartTimeNs,
		EndTimeNs:    r.EndTimeNs,
		StatusError:  r.StatusError,
		SizeBytes:    r.SizeBytes,
		OpAttrs:      r.OpAttrs,
	})
	if err != nil {
		return nil, fmt.Errorf("encode span record: %w", err)
	}
	return data, nil
}

func decodeRecord(data []byte) (SpanRecord, error) {
	var w spanRecordWire
	if err := json.Unmarshal(data, &w); err != nil {
		return SpanRecord{}, fmt.Errorf("decode span record: %w", err)
	}
	traceID, err := parseTraceID(w.TraceID)
	if err != nil {
		return SpanRecord{}, fmt.Errorf("decode span record: %w", err)
	}
	spanID, err := parseSpanID(w.SpanID)
	if err != nil {
		return SpanRecord{}, fmt.Errorf("decode span record: %w", err)
	}
	parentSpanID, err := parseSpanID(w.ParentSpanID)
	if err != nil {
		return SpanRecord{}, fmt.Errorf("decode span record: %w", err)
	}
	return SpanRecord{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentSpanID,
		Service:      w.Service,
		Name:         w.Name,
		Kind:         w.Kind,
		StartTimeNs:  w.StartTimeNs,
		EndTimeNs:    w.EndTimeNs,
		StatusError:  w.StatusError,
		SizeBytes:    w.SizeBytes,
		OpAttrs:      w.OpAttrs,
	}, nil
}

func traceKey(id [16]byte) string {
	return "trace:" + hex.EncodeToString(id[:])
}

func traceIDHex(id [16]byte) string {
	return hex.EncodeToString(id[:])
}

func spanIDHex(id [8]byte) string {
	return hex.EncodeToString(id[:])
}

func parseTraceID(s string) ([16]byte, error) {
	var id [16]byte
	raw, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("parse trace id %q: %w", s, err)
	}
	if len(raw) != len(id) {
		return id, fmt.Errorf("parse trace id %q: want 16 bytes, got %d", s, len(raw))
	}
	copy(id[:], raw)
	return id, nil
}

func parseSpanID(s string) ([8]byte, error) {
	var id [8]byte
	raw, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("parse span id %q: %w", s, err)
	}
	if len(raw) != len(id) {
		return id, fmt.Errorf("parse span id %q: want 8 bytes, got %d", s, len(raw))
	}
	copy(id[:], raw)
	return id, nil
}
