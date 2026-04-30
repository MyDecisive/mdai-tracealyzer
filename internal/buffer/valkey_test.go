package buffer

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestClassifyPutError_NonValkeyErrorIsBackendError(t *testing.T) {
	t.Parallel()

	if got := classifyPutError(errors.New("connection reset")); got != ReasonBackendError {
		t.Errorf("connection reset: got %q, want %q", got, ReasonBackendError)
	}
	if got := classifyPutError(context.Canceled); got != ReasonBackendError {
		t.Errorf("ctx canceled: got %q, want %q", got, ReasonBackendError)
	}
}

func TestMergeFinalizable_MaxTTLWinsOverQuiet(t *testing.T) {
	t.Parallel()

	both := "0102030405060708090a0b0c0d0e0f10"
	onlyQuiet := "1112131415161718191a1b1c1d1e1f20"
	onlyTTL := "2122232425262728292a2b2c2d2e2f30"

	got := mergeFinalizable(
		[]string{both, onlyTTL},
		[]string{both, onlyQuiet},
		zap.NewNop(),
	)

	if len(got) != 3 {
		t.Fatalf("len: want 3, got %d", len(got))
	}

	triggerByHex := make(map[string]string, len(got))
	for _, f := range got {
		triggerByHex[traceIDHex(f.TraceID)] = f.Trigger
	}
	if triggerByHex[both] != TriggerMaxTTL {
		t.Errorf("both: want max_ttl, got %q", triggerByHex[both])
	}
	if triggerByHex[onlyTTL] != TriggerMaxTTL {
		t.Errorf("onlyTTL: want max_ttl, got %q", triggerByHex[onlyTTL])
	}
	if triggerByHex[onlyQuiet] != TriggerQuiet {
		t.Errorf("onlyQuiet: want quiet, got %q", triggerByHex[onlyQuiet])
	}
}

func TestMergeFinalizable_SkipsMalformedHex(t *testing.T) {
	t.Parallel()

	valid := "0102030405060708090a0b0c0d0e0f10"
	got := mergeFinalizable(
		[]string{"zz02030405060708090a0b0c0d0e0f10"},
		[]string{valid, "short"},
		zap.NewNop(),
	)

	if len(got) != 1 {
		t.Fatalf("len: want 1, got %d (%+v)", len(got), got)
	}
	if traceIDHex(got[0].TraceID) != valid {
		t.Errorf("trace: got %x", got[0].TraceID)
	}
	if got[0].Trigger != TriggerQuiet {
		t.Errorf("trigger: want quiet, got %q", got[0].Trigger)
	}
}

func TestMergeFinalizable_Empty(t *testing.T) {
	t.Parallel()

	got := mergeFinalizable(nil, nil, zap.NewNop())
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}

func TestNewValkeyBuffer_Validation(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()

	t.Run("empty addr", func(t *testing.T) {
		t.Parallel()
		if _, err := NewValkeyBuffer(t.Context(), ValkeyOptions{MaxTTL: time.Minute, Logger: logger}); err == nil ||
			!strings.Contains(err.Error(), "addr is required") {
			t.Errorf("want 'addr is required', got %v", err)
		}
	})

	t.Run("zero max_ttl", func(t *testing.T) {
		t.Parallel()
		if _, err := NewValkeyBuffer(t.Context(), ValkeyOptions{Addr: "127.0.0.1:6379", Logger: logger}); err == nil ||
			!strings.Contains(err.Error(), "max_ttl must be > 0") {
			t.Errorf("want 'max_ttl must be > 0', got %v", err)
		}
	})

	t.Run("nil logger", func(t *testing.T) {
		t.Parallel()
		if _, err := NewValkeyBuffer(t.Context(), ValkeyOptions{Addr: "127.0.0.1:6379", MaxTTL: time.Minute}); err == nil ||
			!strings.Contains(err.Error(), "logger is required") {
			t.Errorf("want 'logger is required', got %v", err)
		}
	})
}

func TestSentinelFor(t *testing.T) {
	t.Parallel()

	if !errors.Is(sentinelFor(ReasonOverflow), ErrBufferFull) {
		t.Errorf("overflow: want ErrBufferFull, got %v", sentinelFor(ReasonOverflow))
	}
	if !errors.Is(sentinelFor(ReasonBackendError), ErrBackendUnavailable) {
		t.Errorf("backend: want ErrBackendUnavailable, got %v", sentinelFor(ReasonBackendError))
	}
	if !errors.Is(sentinelFor("anything-else"), ErrBackendUnavailable) {
		t.Errorf("default: want ErrBackendUnavailable, got %v", sentinelFor("anything-else"))
	}
}

func TestPut_HappyPath(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	m.client.EXPECT().
		DoMulti(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(putReply(mock.Result(mock.ValkeyString("OK"))))
	if err := m.buf.Put(t.Context(), sampleSpan(1)); err != nil {
		t.Errorf("Put: %v", err)
	}
}

func TestPut_BackendError(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	m.client.EXPECT().
		DoMulti(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(putReply(mock.ErrorResult(errors.New("connection reset"))))

	err := m.buf.Put(t.Context(), sampleSpan(2))
	if !errors.Is(err, ErrBackendUnavailable) {
		t.Errorf("want ErrBackendUnavailable, got %v", err)
	}
	if got := counterValue(t, m.reg, "topology_buffer_rejected_total", "reason", "backend_error"); got != 1 {
		t.Errorf("rejected backend_error: want 1, got %v", got)
	}
}

// TestPut_OOM hits classifyPutError's OOM branch without a real Valkey.
// mock.ValkeyError produces a typeSimpleErr message; ValkeyResult.Error()
// converts that to *valkey.ValkeyError, which valkey.IsValkeyErr recognizes.
func TestPut_OOM(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	m.client.EXPECT().
		DoMulti(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(putReply(mock.Result(mock.ValkeyError("OOM command not allowed when used memory > 'maxmemory'."))))

	err := m.buf.Put(t.Context(), sampleSpan(3))
	if !errors.Is(err, ErrBufferFull) {
		t.Errorf("want ErrBufferFull, got %v", err)
	}
	if got := counterValue(t, m.reg, "topology_buffer_rejected_total", "reason", "overflow"); got != 1 {
		t.Errorf("rejected overflow: want 1, got %v", got)
	}
}

func TestScan_HappyPath(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)

	traceQuietOnly := "0102030405060708090a0b0c0d0e0f10"
	traceTTL := "1112131415161718191a1b1c1d1e1f20"

	m.client.EXPECT().DoMulti(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]valkey.ValkeyResult{
			mock.Result(mock.ValkeyArray(mock.ValkeyBlobString(traceQuietOnly))),
			mock.Result(mock.ValkeyArray(mock.ValkeyBlobString(traceTTL))),
		})

	now := time.Now()
	got, err := m.buf.Scan(t.Context(), now, now)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len: want 2, got %d", len(got))
	}

	byHex := map[string]string{}
	for _, f := range got {
		byHex[traceIDHex(f.TraceID)] = f.Trigger
	}
	if byHex[traceQuietOnly] != TriggerQuiet {
		t.Errorf("quiet-only trigger: want quiet, got %q", byHex[traceQuietOnly])
	}
	if byHex[traceTTL] != TriggerMaxTTL {
		t.Errorf("ttl trigger: want max_ttl, got %q", byHex[traceTTL])
	}
}

func TestScan_Errors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		errSlot int
		wrap    string
	}{
		{name: "last_write", errSlot: 0, wrap: "valkey scan last_write"},
		{name: "first_seen", errSlot: 1, wrap: "valkey scan first_seen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newMockedBuffer(t)
			results := []valkey.ValkeyResult{
				mock.Result(mock.ValkeyArray()),
				mock.Result(mock.ValkeyArray()),
			}
			results[tc.errSlot] = mock.ErrorResult(errors.New("boom"))
			m.client.EXPECT().DoMulti(gomock.Any(), gomock.Any(), gomock.Any()).Return(results)

			_, err := m.buf.Scan(t.Context(), time.Now(), time.Now())
			if err == nil || !strings.Contains(err.Error(), tc.wrap) {
				t.Errorf("want %q wrap, got %v", tc.wrap, err)
			}
		})
	}
}

func TestDrain_HappyPath(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	dedMock := expectDedicated(t, m)

	record := SpanRecord{
		TraceID:     [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:      [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Service:     "svc",
		Name:        "op",
		StartTimeNs: 1,
		EndTimeNs:   2,
	}
	encoded, err := encodeRecord(record)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	spanHex := spanIDHex(record.SpanID)

	dedMock.EXPECT().DoMulti(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(queuedExecReply(mock.ValkeyArray(
		mock.ValkeyMap(map[string]valkey.ValkeyMessage{
			spanHex: mock.ValkeyBlobString(string(encoded)),
		}),
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(1),
	)))

	got, err := m.buf.Drain(t.Context(), record.TraceID)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len: want 1, got %d", len(got))
	}
	if got[spanHex].Name != "op" {
		t.Errorf("decoded name: got %q, want op", got[spanHex].Name)
	}
}

func TestDrain_DecodeError_IncrementsCounter(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	dedMock := expectDedicated(t, m)

	validRecord := SpanRecord{SpanID: [8]byte{9}, Name: "ok"}
	encoded, err := encodeRecord(validRecord)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}

	dedMock.EXPECT().DoMulti(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(queuedExecReply(mock.ValkeyArray(
		mock.ValkeyMap(map[string]valkey.ValkeyMessage{
			"aa00000000000000": mock.ValkeyBlobString(string(encoded)),
			"bb00000000000000": mock.ValkeyBlobString("not-json"),
		}),
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(1),
	)))

	var tid [16]byte
	got, err := m.buf.Drain(t.Context(), tid)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("decoded records: want 1, got %d", len(got))
	}
	if v := testutil.ToFloat64(m.decode); v != 1 {
		t.Errorf("decode counter: want 1, got %v", v)
	}
}

func TestDrain_ExecError(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	dedMock := expectDedicated(t, m)

	dedMock.EXPECT().DoMulti(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return([]valkey.ValkeyResult{
		mock.Result(mock.ValkeyString("OK")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.ErrorResult(errors.New("EXECABORT")),
	})

	var tid [16]byte
	if _, err := m.buf.Drain(t.Context(), tid); err == nil ||
		!strings.Contains(err.Error(), "valkey drain exec") {
		t.Errorf("want 'valkey drain exec' wrap, got %v", err)
	}
}

func TestDrain_EmptyTrace(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	core, logs := observer.New(zap.DebugLevel)
	m.buf.logger = zap.New(core)
	dedMock := expectDedicated(t, m)

	dedMock.EXPECT().DoMulti(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(queuedExecReply(mock.ValkeyArray(
		mock.ValkeyMap(map[string]valkey.ValkeyMessage{}),
		mock.ValkeyInt64(0),
		mock.ValkeyInt64(0),
		mock.ValkeyInt64(0),
	)))

	var tid [16]byte
	got, err := m.buf.Drain(t.Context(), tid)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want empty map, got %+v", got)
	}
	if n := logs.FilterMessage("drain: trace already absent").Len(); n != 1 {
		t.Errorf("want 1 debug log for empty drain, got %d (all: %v)", n, logs.All())
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	m.client.EXPECT().Close()
	m.buf.Close()
}

// TestDrain_AtomicAgainstConcurrentPut drives a concurrent Put while Drain
// runs on the same trace_id and asserts no span silently vanishes — each
// Put'd span must either be returned by Drain or still be present in the
// buffer after Drain returns.
//
// Requires a reachable Valkey/Redis; set TRACEALYZER_TEST_VALKEY_ADDR=host:port
// to run. Skipped otherwise.
// TODO make a testcontainer integration test after v1.
func TestDrain_AtomicAgainstConcurrentPut(t *testing.T) {
	addr := os.Getenv("TRACEALYZER_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("set TRACEALYZER_TEST_VALKEY_ADDR=host:port to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buf, err := NewValkeyBuffer(ctx, ValkeyOptions{
		Addr:     addr,
		Password: os.Getenv("TRACEALYZER_TEST_VALKEY_PASSWORD"),
		MaxTTL:   time.Minute,
		Logger:   zap.NewNop(),
	})
	if err != nil {
		t.Fatalf("NewValkeyBuffer: %v", err)
	}
	t.Cleanup(buf.Close)

	var traceID [16]byte
	if _, randErr := rand.Read(traceID[:]); randErr != nil {
		t.Fatalf("rand trace id: %v", randErr)
	}

	// Seed a handful of spans so Drain has non-trivial work.
	seed := uint32(32)
	putSpan := func(i uint32) SpanRecord {
		var sid [8]byte
		sid[0] = byte((i >> 24) & 0xff)
		sid[1] = byte((i >> 16) & 0xff)
		sid[2] = byte((i >> 8) & 0xff)
		sid[3] = byte(i & 0xff)
		return SpanRecord{
			TraceID:     traceID,
			SpanID:      sid,
			Service:     "race-test",
			Name:        "span",
			StartTimeNs: int64(i),
			EndTimeNs:   int64(i) + 1,
		}
	}
	for i := range seed {
		if putErr := buf.Put(ctx, putSpan(i)); putErr != nil {
			t.Fatalf("seed Put %d: %v", i, putErr)
		}
	}

	// Hammer Put on the same trace from another goroutine while Drain runs.
	// We pick IDs in a disjoint range so the assertion can match on span_id.
	var wg sync.WaitGroup
	var putCount atomic.Uint32
	putCount.Store(seed)
	stop := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			next := putCount.Add(1) - 1
			if putErr := buf.Put(ctx, putSpan(next)); putErr != nil {
				t.Errorf("concurrent Put %d: %v", next, putErr)
				return
			}
		}
	})

	// Let the writer get going, then drain.
	time.Sleep(5 * time.Millisecond)
	drained, err := buf.Drain(ctx, traceID)
	if err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("Drain: %v", err)
	}
	close(stop)
	wg.Wait()

	// Anything not returned by Drain must still be in the buffer. Re-fetch
	// whatever remains by doing a second Drain — with atomicity, all late
	// Puts land after the first Drain's EXEC, so they end up in the second.
	remaining, err := buf.Drain(ctx, traceID)
	if err != nil {
		t.Fatalf("second Drain: %v", err)
	}

	total := len(drained) + len(remaining)
	produced := putCount.Load()
	if total != int(produced) {
		// Report which span IDs are missing.
		present := make(map[string]bool, total)
		for k := range drained {
			present[k] = true
		}
		for k := range remaining {
			present[k] = true
		}
		missing := 0
		for i := range produced {
			sid := spanIDHex(putSpan(i).SpanID)
			if !present[sid] {
				missing++
			}
		}
		t.Fatalf("spans lost: produced=%d, drained=%d, remaining=%d, missing=%d",
			produced, len(drained), len(remaining), missing)
	}
}

type mockedBuffer struct {
	buf    *ValkeyBuffer
	client *mock.Client
	reg    *prometheus.Registry
	decode prometheus.Counter
}

// newMockedBuffer wires a ValkeyBuffer to a gomock-backed valkey.Client.
// The registry and decode counter let tests assert metric side effects
// without touching the global default Prometheus registry.
func newMockedBuffer(t *testing.T) mockedBuffer {
	t.Helper()
	client := mock.NewClient(gomock.NewController(t))
	reg := prometheus.NewRegistry()
	decode := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_decode_errors_total",
		Help: "test-only counter for decode-error assertions",
	})
	reg.MustRegister(decode)
	buf := newValkeyBufferFromClient(client, time.Minute, NewMetrics(reg, decode), zap.NewNop())
	return mockedBuffer{buf: buf, client: client, reg: reg, decode: decode}
}

// queuedExecReply expands a DoMulti reply for the Drain transaction:
// MULTI+QUEUED*4 placeholders followed by the EXEC payload.
func queuedExecReply(execPayload valkey.ValkeyMessage) []valkey.ValkeyResult {
	return []valkey.ValkeyResult{
		mock.Result(mock.ValkeyString("OK")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.Result(mock.ValkeyString("QUEUED")),
		mock.Result(execPayload),
	}
}

// expectDedicated wires m.client.Dedicated to invoke the caller's callback
// on a fresh mock.DedicatedClient, which the caller configures with its
// own EXPECT().DoMulti(...) for the transaction.
func expectDedicated(t *testing.T, m mockedBuffer) *mock.DedicatedClient {
	t.Helper()
	dedMock := mock.NewDedicatedClient(gomock.NewController(t))
	m.client.EXPECT().Dedicated(gomock.Any()).DoAndReturn(
		func(cb func(valkey.DedicatedClient) error) error { return cb(dedMock) },
	)
	return dedMock
}

// putReply expands a Put's DoMulti reply: first slot is the caller-supplied
// result (for HSET success/failure) followed by three OK placeholders for
// PEXPIRE and the two ZADDs.
func putReply(first valkey.ValkeyResult) []valkey.ValkeyResult {
	return []valkey.ValkeyResult{
		first,
		mock.Result(mock.ValkeyInt64(1)),
		mock.Result(mock.ValkeyInt64(1)),
		mock.Result(mock.ValkeyInt64(1)),
	}
}

func sampleSpan(seed byte) SpanRecord {
	var tid [16]byte
	tid[0] = seed
	var sid [8]byte
	sid[0] = seed
	return SpanRecord{
		TraceID:     tid,
		SpanID:      sid,
		Service:     "test",
		Name:        "op",
		StartTimeNs: 1,
		EndTimeNs:   2,
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) float64 {
	t.Helper()
	if len(labelKV)%2 != 0 {
		t.Fatalf("counterValue: odd labelKV length %d", len(labelKV))
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if !labelsMatch(metric.GetLabel(), labelKV) {
				continue
			}
			return metric.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %q with labels %v not in registry", name, labelKV)
	return 0
}

func labelsMatch(labels []*dto.LabelPair, labelKV []string) bool {
	for i := 0; i < len(labelKV); i += 2 {
		k, v := labelKV[i], labelKV[i+1]
		found := false
		for _, l := range labels {
			if l.GetName() == k && l.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
