package buffer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

const (
	zLastWriteKey      = "traces:last_write"
	zFirstSeenKey      = "traces:first_seen"
	waitInitialBackoff = 10 * time.Millisecond
	waitBackoffCap     = 30 * time.Second
	waitBackoffFactor  = 2
)

// ValkeyBuffer is the production Buffer backed by a Valkey instance.
// It satisfies the Buffer interface defined in buffer.go.
type ValkeyBuffer struct {
	client  valkey.Client
	maxTTL  time.Duration
	metrics *Metrics
	logger  *zap.Logger
}

type ValkeyOptions struct {
	Addr     string
	DB       int
	Password string
	MaxTTL   time.Duration
	Metrics  *Metrics
	Logger   *zap.Logger
}

// NewValkeyBuffer constructs the buffer but does not block on Valkey
// reachability. Callers invoke WaitUntilReady to confirm connectivity
// before flipping the readiness gate.
func NewValkeyBuffer(opts ValkeyOptions) (*ValkeyBuffer, error) {
	if opts.Addr == "" {
		return nil, errors.New("valkey buffer: addr is required")
	}
	if opts.MaxTTL <= 0 {
		return nil, errors.New("valkey buffer: max_ttl must be > 0")
	}
	if opts.Logger == nil {
		return nil, errors.New("valkey buffer: logger is required")
	}

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{opts.Addr},
		SelectDB:    opts.DB,
		Password:    opts.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("valkey client: %w", err)
	}
	return newValkeyBufferFromClient(client, opts.MaxTTL, opts.Metrics, opts.Logger), nil
}

func newValkeyBufferFromClient(client valkey.Client, maxTTL time.Duration, metrics *Metrics, logger *zap.Logger) *ValkeyBuffer {
	return &ValkeyBuffer{
		client:  client,
		maxTTL:  maxTTL,
		metrics: metrics,
		logger:  logger,
	}
}

// Ping runs a single PING against Valkey.
func (b *ValkeyBuffer) Ping(ctx context.Context) error {
	if err := b.client.Do(ctx, b.client.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("valkey ping: %w", err)
	}
	return nil
}

// WaitUntilReady pings Valkey with exponential backoff until either the
// ping succeeds or ctx is cancelled. It returns ctx.Err() on cancellation
// and nil on success. Each failed attempt is logged at warn level.
func (b *ValkeyBuffer) WaitUntilReady(ctx context.Context) error {
	backoff := waitInitialBackoff
	for {
		err := b.Ping(ctx)
		if err == nil {
			return nil
		}
		b.logger.Warn("valkey ping failed, retrying",
			zap.Error(err), zap.Duration("next_attempt_in", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*waitBackoffFactor, waitBackoffCap)
	}
}

// Put pipelines HSET + PEXPIRE + ZADD last_write + ZADD NX first_seen.
// Returned errors are wrapped with a sentinel from buffer.go so callers
// can classify via errors.Is without parsing driver strings:
//   - encoding failures → ErrInvalidSpan (counter not bumped; service bug)
//   - Valkey OOM        → ErrBufferFull (counter reason="overflow")
//   - other backend     → ErrBackendUnavailable (counter reason="backend_error")
func (b *ValkeyBuffer) Put(ctx context.Context, r SpanRecord) error {
	data, err := encodeRecord(r)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSpan, err)
	}

	key := traceKey(r.TraceID)
	field := spanIDHex(r.SpanID)
	traceHex := traceIDHex(r.TraceID)
	ttlMillis := b.maxTTL.Milliseconds()
	nowNs := time.Now().UnixNano()
	// nowScore loses sub-microsecond precision (UnixNano > 2^53), but Scan's
	// ZRANGEBYSCORE bounds round identically, so quiet/TTL cutoffs at
	// second-scale stay correct. Switch to seconds if sub-second cutoffs ever
	// become real.
	nowScore := float64(nowNs)

	cmds := []valkey.Completed{
		b.client.B().Hset().Key(key).FieldValue().FieldValue(field, string(data)).Build(),
		b.client.B().Pexpire().Key(key).Milliseconds(ttlMillis).Build(),
		b.client.B().Zadd().Key(zLastWriteKey).ScoreMember().ScoreMember(nowScore, traceHex).Build(),
		b.client.B().Zadd().Key(zFirstSeenKey).Nx().ScoreMember().ScoreMember(nowScore, traceHex).Build(),
	}

	for _, resp := range b.client.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			reason := classifyPutError(err)
			b.metrics.incRejected(reason)
			return fmt.Errorf("%w: %w", sentinelFor(reason), err)
		}
	}
	b.logger.Debug("span stored", zap.String("trace_id", traceHex), zap.String("span_id", field))
	return nil
}

// Scan returns trace IDs past either cutoff, tagged with the trigger
// that caught them. max_ttl wins when a trace is past both cutoffs.
func (b *ValkeyBuffer) Scan(ctx context.Context, quietCutoff, ttlCutoff time.Time) ([]Finalizable, error) {
	cmds := []valkey.Completed{
		b.client.B().Zrangebyscore().Key(zLastWriteKey).
			Min("-inf").Max(strconv.FormatInt(quietCutoff.UnixNano(), 10)).Build(),
		b.client.B().Zrangebyscore().Key(zFirstSeenKey).
			Min("-inf").Max(strconv.FormatInt(ttlCutoff.UnixNano(), 10)).Build(),
	}
	results := b.client.DoMulti(ctx, cmds...)

	quietMembers, err := results[0].AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("valkey scan last_write: %w", err)
	}
	ttlMembers, err := results[1].AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("valkey scan first_seen: %w", err)
	}

	return mergeFinalizable(ttlMembers, quietMembers, b.logger), nil
}

// Drain atomically fetches all spans for a trace and removes its buffer
// state. Atomicity is provided by MULTI/EXEC on a dedicated connection,
// so a concurrent Put on another connection cannot interleave between
// HGETALL and DEL. Returned map keys are hex-encoded span IDs.
func (b *ValkeyBuffer) Drain(ctx context.Context, traceID [16]byte) (map[string]SpanRecord, error) {
	key := traceKey(traceID)
	traceHex := traceIDHex(traceID)

	var raw map[string]string
	dedErr := b.client.Dedicated(func(c valkey.DedicatedClient) error {
		cmds := []valkey.Completed{
			c.B().Multi().Build(),
			c.B().Hgetall().Key(key).Build(),
			c.B().Del().Key(key).Build(),
			c.B().Zrem().Key(zLastWriteKey).Member(traceHex).Build(),
			c.B().Zrem().Key(zFirstSeenKey).Member(traceHex).Build(),
			c.B().Exec().Build(),
		}
		results := c.DoMulti(ctx, cmds...)

		// EXEC is the last reply; its payload is the array of real per-command
		// responses (HGETALL, DEL, ZREM, ZREM). The prior replies are MULTI's
		// "OK" and four "QUEUED" placeholders.
		execReply := results[len(results)-1]
		if err := execReply.Error(); err != nil {
			return fmt.Errorf("valkey drain exec: %w", err)
		}
		replies, err := execReply.ToArray()
		if err != nil {
			return fmt.Errorf("valkey drain exec reply: %w", err)
		}
		m, err := replies[0].AsStrMap()
		if err != nil {
			return fmt.Errorf("valkey drain hgetall: %w", err)
		}
		raw = m
		return nil
	})
	if dedErr != nil {
		return nil, dedErr
	}

	out := make(map[string]SpanRecord, len(raw))
	for spanHex, encoded := range raw {
		record, err := decodeRecord([]byte(encoded))
		if err != nil {
			b.metrics.incDecodeErrors()
			b.logger.Warn("drain: decode span record",
				zap.String("trace_id", traceHex),
				zap.String("span_id", spanHex),
				zap.Error(err))
			continue
		}
		out[spanHex] = record
	}
	return out, nil
}

func (b *ValkeyBuffer) Close() {
	b.client.Close()
}

// classifyPutError splits Valkey Put failures into overflow (maxmemory
// rejection) and backend_error (everything else). Valkey returns OOM as a
// server ERR reply whose message starts with "OOM "; connection/protocol
// failures surface as non-ValkeyError values.
func classifyPutError(err error) RejectReason {
	if re, ok := valkey.IsValkeyErr(err); ok && strings.HasPrefix(re.Error(), "OOM ") {
		return ReasonOverflow
	}
	return ReasonBackendError
}

func sentinelFor(reason RejectReason) error {
	if reason == ReasonOverflow {
		return ErrBufferFull
	}
	return ErrBackendUnavailable
}

// mergeFinalizable deduplicates trace IDs across the two cutoff sets.
// max_ttl is the stronger signal; a trace present in both is tagged
// TriggerMaxTTL. Malformed hex members are dropped with a warn log.
func mergeFinalizable(ttlMembers, quietMembers []string, logger *zap.Logger) []Finalizable {
	seen := make(map[string]struct{}, len(ttlMembers)+len(quietMembers))
	out := make([]Finalizable, 0, len(ttlMembers)+len(quietMembers))

	for _, hex := range ttlMembers {
		if _, ok := seen[hex]; ok {
			continue
		}
		id, err := parseTraceID(hex)
		if err != nil {
			logger.Warn("scan: malformed trace id in first_seen", zap.String("member", hex), zap.Error(err))
			continue
		}
		seen[hex] = struct{}{}
		out = append(out, Finalizable{TraceID: id, Trigger: TriggerMaxTTL})
	}
	for _, hex := range quietMembers {
		if _, ok := seen[hex]; ok {
			continue
		}
		id, err := parseTraceID(hex)
		if err != nil {
			logger.Warn("scan: malformed trace id in last_write", zap.String("member", hex), zap.Error(err))
			continue
		}
		seen[hex] = struct{}{}
		out = append(out, Finalizable{TraceID: id, Trigger: TriggerQuiet})
	}
	return out
}
