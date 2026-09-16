package buffer

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestClassifyPutError_NonValkeyErrorIsBackendError(t *testing.T) {
	t.Parallel()

	assert.Equal(t, ReasonBackendError, classifyPutError(errors.New("connection reset")))
	assert.Equal(t, ReasonBackendError, classifyPutError(context.Canceled))
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

	require.Len(t, got, 3)

	triggerByHex := make(map[string]string, len(got))
	for _, f := range got {
		triggerByHex[traceIDHex(f.TraceID)] = f.Trigger
	}
	assert.Equal(t, TriggerMaxTTL, triggerByHex[both], "both")
	assert.Equal(t, TriggerMaxTTL, triggerByHex[onlyTTL], "onlyTTL")
	assert.Equal(t, TriggerQuiet, triggerByHex[onlyQuiet], "onlyQuiet")
}

func TestMergeFinalizable_SkipsMalformedHex(t *testing.T) {
	t.Parallel()

	valid := "0102030405060708090a0b0c0d0e0f10"
	got := mergeFinalizable(
		[]string{"zz02030405060708090a0b0c0d0e0f10"},
		[]string{valid, "short"},
		zap.NewNop(),
	)

	require.Len(t, got, 1)
	assert.Equal(t, valid, traceIDHex(got[0].TraceID))
	assert.Equal(t, TriggerQuiet, got[0].Trigger)
}

func TestMergeFinalizable_Empty(t *testing.T) {
	t.Parallel()

	got := mergeFinalizable(nil, nil, zap.NewNop())
	require.Empty(t, got)
}

func TestNewValkeyBuffer_Validation(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()

	t.Run("empty addr", func(t *testing.T) {
		t.Parallel()
		_, err := NewValkeyBuffer(t.Context(), ValkeyOptions{MaxTTL: time.Minute, OperationTimeout: time.Second, Logger: logger})
		require.ErrorContains(t, err, "addr is required")
	})

	t.Run("zero max_ttl", func(t *testing.T) {
		t.Parallel()
		_, err := NewValkeyBuffer(t.Context(), ValkeyOptions{Addr: "127.0.0.1:6379", OperationTimeout: time.Second, Logger: logger})
		require.ErrorContains(t, err, "max_ttl must be > 0")
	})

	t.Run("zero operation_timeout", func(t *testing.T) {
		t.Parallel()
		_, err := NewValkeyBuffer(t.Context(), ValkeyOptions{Addr: "127.0.0.1:6379", MaxTTL: time.Minute, Logger: logger})
		require.ErrorContains(t, err, "operation_timeout must be > 0")
	})

	t.Run("nil logger", func(t *testing.T) {
		t.Parallel()
		_, err := NewValkeyBuffer(t.Context(), ValkeyOptions{Addr: "127.0.0.1:6379", MaxTTL: time.Minute, OperationTimeout: time.Second})
		require.ErrorContains(t, err, "logger is required")
	})
}

// Picks an address that is guaranteed to refuse: bind a listener, capture its
// addr, then close it. Subsequent dials hit "connection refused" and the
// retry loop runs until ctx expires.
func unreachableAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	addr := l.Addr().String()
	require.NoError(t, l.Close(), "close listener")
	return addr
}

func TestNewValkeyBuffer_DialRetriesUntilCtxDone(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.WarnLevel)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err := NewValkeyBuffer(ctx, ValkeyOptions{
		Addr:             unreachableAddr(t),
		MaxTTL:           time.Minute,
		OperationTimeout: time.Second,
		Logger:           zap.New(core),
	})
	require.ErrorContains(t, err, "valkey client")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Positive(t, logs.FilterMessage("valkey dial failed, retrying").Len(), "expected at least one retry warn log")
}

func TestSentinelFor(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, sentinelFor(ReasonOverflow), ErrBufferFull)
	require.ErrorIs(t, sentinelFor(ReasonBackendError), ErrBackendUnavailable)
	require.ErrorIs(t, sentinelFor("anything-else"), ErrBackendUnavailable, "default")
}

func TestPut_HappyPath(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	m.client.EXPECT().
		DoMulti(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(putReply(mock.Result(mock.ValkeyString("OK"))))
	require.NoError(t, m.buf.Put(t.Context(), sampleSpan(1)))
}

func TestPut_BackendError(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	m.client.EXPECT().
		DoMulti(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(putReply(mock.ErrorResult(errors.New("connection reset"))))

	err := m.buf.Put(t.Context(), sampleSpan(2))
	require.ErrorIs(t, err, ErrBackendUnavailable)
	assert.InDelta(t, 1, counterValue(t, m.reg, "topology_buffer_rejected_total", "reason", "backend_error"), 0)
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
	require.ErrorIs(t, err, ErrBufferFull)
	assert.InDelta(t, 1, counterValue(t, m.reg, "topology_buffer_rejected_total", "reason", "overflow"), 0)
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
	require.NoError(t, err)
	require.Len(t, got, 2)

	byHex := map[string]string{}
	for _, f := range got {
		byHex[traceIDHex(f.TraceID)] = f.Trigger
	}
	assert.Equal(t, TriggerQuiet, byHex[traceQuietOnly], "quiet-only trigger")
	assert.Equal(t, TriggerMaxTTL, byHex[traceTTL], "ttl trigger")
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
			require.ErrorContains(t, err, tc.wrap)
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
	require.NoError(t, err)
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
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "op", got[spanHex].Name)
}

func TestDrain_DecodeError_IncrementsCounter(t *testing.T) {
	t.Parallel()
	m := newMockedBuffer(t)
	dedMock := expectDedicated(t, m)

	validRecord := SpanRecord{SpanID: [8]byte{9}, Name: "ok"}
	encoded, err := encodeRecord(validRecord)
	require.NoError(t, err)

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
	require.NoError(t, err)
	assert.Len(t, got, 1, "decoded records")
	assert.InDelta(t, 1, testutil.ToFloat64(m.decode), 0, "decode counter")
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
	_, err := m.buf.Drain(t.Context(), tid)
	require.ErrorContains(t, err, "valkey drain exec")
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
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 1, logs.FilterMessage("drain: trace already absent").Len(), "debug log for empty drain (all: %v)", logs.All())
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
		Addr:             addr,
		Password:         os.Getenv("TRACEALYZER_TEST_VALKEY_PASSWORD"),
		MaxTTL:           time.Minute,
		OperationTimeout: 5 * time.Second,
		Logger:           zap.NewNop(),
	})
	require.NoError(t, err)
	t.Cleanup(buf.Close)

	var traceID [16]byte
	_, err = rand.Read(traceID[:])
	require.NoError(t, err, "rand trace id")

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
		require.NoError(t, buf.Put(ctx, putSpan(i)), "seed Put %d", i)
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
			if !assert.NoError(t, buf.Put(ctx, putSpan(next)), "concurrent Put %d", next) {
				return
			}
		}
	})

	// Let the writer get going, then drain.
	time.Sleep(5 * time.Millisecond)
	drained, err := buf.Drain(ctx, traceID)
	close(stop)
	wg.Wait()
	require.NoError(t, err, "Drain")

	// Anything not returned by Drain must still be in the buffer. Re-fetch
	// whatever remains by doing a second Drain — with atomicity, all late
	// Puts land after the first Drain's EXEC, so they end up in the second.
	remaining, err := buf.Drain(ctx, traceID)
	require.NoError(t, err, "second Drain")

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
		require.Failf(t, "spans lost", "produced=%d, drained=%d, remaining=%d, missing=%d",
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
	require.Zero(t, len(labelKV)%2, "counterValue: odd labelKV length %d", len(labelKV))
	families, err := reg.Gather()
	require.NoError(t, err, "Gather")
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
	require.Failf(t, "metric not in registry", "%q with labels %v", name, labelKV)
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
