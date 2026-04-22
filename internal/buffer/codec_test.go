package buffer

import (
	"reflect"
	"testing"
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
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := encodeRecord(tc.in)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := decodeRecord(data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.in) {
				t.Fatalf("round-trip mismatch:\nwant %+v\n got %+v", tc.in, got)
			}
		})
	}
}

func TestTraceKey_AndHex(t *testing.T) {
	t.Parallel()

	id := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	wantHex := "0102030405060708090a0b0c0d0e0f10"

	if got := traceIDHex(id); got != wantHex {
		t.Errorf("traceIDHex: want %q, got %q", wantHex, got)
	}
	if got := traceKey(id); got != "trace:"+wantHex {
		t.Errorf("traceKey: want %q, got %q", "trace:"+wantHex, got)
	}
}

func TestSpanIDHex(t *testing.T) {
	t.Parallel()

	if got := spanIDHex([8]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}); got != "1122334455667788" {
		t.Errorf("spanIDHex: got %q", got)
	}
}

func TestParseTraceID(t *testing.T) {
	t.Parallel()

	hex := "0102030405060708090a0b0c0d0e0f10"
	want := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	got, err := parseTraceID(hex)
	if err != nil {
		t.Fatalf("parseTraceID: %v", err)
	}
	if got != want {
		t.Errorf("parseTraceID mismatch: want %x, got %x", want, got)
	}

	if _, err := parseTraceID("short"); err == nil {
		t.Error("parseTraceID short: want error")
	}
	if _, err := parseTraceID("zz02030405060708090a0b0c0d0e0f10"); err == nil {
		t.Error("parseTraceID bad hex: want error")
	}
}
