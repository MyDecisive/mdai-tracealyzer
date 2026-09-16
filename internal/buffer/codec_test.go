package buffer

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   SpanRecord
	}{
		{
			name: "root with op attrs",
			in: SpanRecord{
				TraceID:      [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
				SpanID:       [8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
				ParentSpanID: [8]byte{},
				Service:      "checkout",
				Name:         "POST /cart",
				Kind:         2,
				StartTimeNs:  1700000000000000000,
				EndTimeNs:    1700000000250000000,
				StatusError:  true,
				SizeBytes:    1234,
				OpAttrs: map[string]string{
					"http.request.method": "POST",
					"http.route":          "/cart",
				},
			},
		},
		{
			name: "child span, nil op attrs",
			in: SpanRecord{
				TraceID:      [16]byte{0xaa},
				SpanID:       [8]byte{0xbb},
				ParentSpanID: [8]byte{0xcc},
				Service:      "db",
				Name:         "SELECT",
				Kind:         3,
				StartTimeNs:  1,
				EndTimeNs:    2,
				SizeBytes:    87,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := encodeRecord(tc.in)
			require.NoError(t, err)
			got, err := decodeRecord(data)
			require.NoError(t, err)
			require.Equal(t, tc.in, got)
		})
	}
}

func TestEncodeRecord_HexWireFormat(t *testing.T) {
	t.Parallel()

	in := SpanRecord{
		TraceID:      [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:       [8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		ParentSpanID: [8]byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28},
		Service:      "checkout",
	}
	data, err := encodeRecord(in)
	require.NoError(t, err)

	var wire struct {
		TraceID      string `json:"trace_id"`
		SpanID       string `json:"span_id"`
		ParentSpanID string `json:"parent_span_id"`
	}
	require.NoError(t, json.Unmarshal(data, &wire), "unmarshal wire (data=%s)", data)
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", wire.TraceID)
	assert.Equal(t, "1112131415161718", wire.SpanID)
	assert.Equal(t, "2122232425262728", wire.ParentSpanID)
}

func TestDecodeRecord_BadHex(t *testing.T) {
	t.Parallel()

	bad := []byte(`{"trace_id":"zz","span_id":"1112131415161718","parent_span_id":"2122232425262728"}`)
	_, err := decodeRecord(bad)
	require.Error(t, err, "want error for non-hex trace_id")
}

func TestParseSpanID(t *testing.T) {
	t.Parallel()

	want := [8]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	got, err := parseSpanID("1122334455667788")
	require.NoError(t, err)
	assert.Equal(t, want, got)

	_, err = parseSpanID("1122")
	require.Error(t, err, "short input")
	_, err = parseSpanID("zz22334455667788")
	require.Error(t, err, "bad hex")
}

func TestTraceKey_AndHex(t *testing.T) {
	t.Parallel()

	id := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	wantHex := "0102030405060708090a0b0c0d0e0f10"

	assert.Equal(t, wantHex, traceIDHex(id))
	assert.Equal(t, "trace:"+wantHex, traceKey(id))
}

func TestSpanIDHex(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "1122334455667788", spanIDHex([8]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}))
}

func TestParseTraceID(t *testing.T) {
	t.Parallel()

	hex := "0102030405060708090a0b0c0d0e0f10"
	want := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	got, err := parseTraceID(hex)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	_, err = parseTraceID("short")
	require.Error(t, err, "short input")
	_, err = parseTraceID("zz02030405060708090a0b0c0d0e0f10")
	require.Error(t, err, "bad hex")
}
