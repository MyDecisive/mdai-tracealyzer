package buffer

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
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

	buf, err := NewValkeyBuffer(ValkeyOptions{
		Addr:     addr,
		Password: os.Getenv("TRACEALYZER_TEST_VALKEY_PASSWORD"),
		MaxTTL:   time.Minute,
		Logger:   zap.NewNop(),
	})
	if err != nil {
		t.Fatalf("NewValkeyBuffer: %v", err)
	}
	defer buf.Close()

	if waitErr := buf.WaitUntilReady(ctx); waitErr != nil {
		t.Fatalf("WaitUntilReady: %v", waitErr)
	}

	var traceID [16]byte
	if _, randErr := rand.Read(traceID[:]); randErr != nil {
		t.Fatalf("rand trace id: %v", randErr)
	}

	// Seed a handful of spans so Drain has non-trivial work.
	seed := 32
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
		if putErr := buf.Put(ctx, putSpan(uint32(i))); putErr != nil {
			t.Fatalf("seed Put %d: %v", i, putErr)
		}
	}

	// Hammer Put on the same trace from another goroutine while Drain runs.
	// We pick IDs in a disjoint range so the assertion can match on span_id.
	var wg sync.WaitGroup
	var putCount atomic.Uint32
	putCount.Store(uint32(seed))
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
	produced := int(putCount.Load())
	if total != produced {
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
			sid := spanIDHex(putSpan(uint32(i)).SpanID)
			if !present[sid] {
				missing++
			}
		}
		t.Fatalf("spans lost: produced=%d, drained=%d, remaining=%d, missing=%d",
			produced, len(drained), len(remaining), missing)
	}
}
