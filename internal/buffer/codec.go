package buffer

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

func encodeRecord(r SpanRecord) ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode span record: %w", err)
	}
	return data, nil
}

func decodeRecord(data []byte) (SpanRecord, error) {
	var r SpanRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return SpanRecord{}, fmt.Errorf("decode span record: %w", err)
	}
	return r, nil
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
