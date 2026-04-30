package buffer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

const (
	zLastWriteKey      = "traces:last_write"
	zFirstSeenKey      = "traces:first_seen"
	dialInitialBackoff = 100 * time.Millisecond
	dialMaxBackoff     = 5 * time.Second
)

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

// NewValkeyBuffer blocks until the initial dial succeeds or ctx is cancelled.
// valkey-go owns reconnection on subsequent connection drops.
func NewValkeyBuffer(ctx context.Context, opts ValkeyOptions) (*ValkeyBuffer, error) {
	if opts.Addr == "" {
		return nil, errors.New("valkey buffer: addr is required")
	}
	if opts.MaxTTL <= 0 {
		return nil, errors.New("valkey buffer: max_ttl must be > 0")
	}
	if opts.Logger == nil {
		return nil, errors.New("valkey buffer: logger is required")
	}

	client, err := dialWithBackoff(ctx, opts)
	if err != nil {
		return nil, err
	}
	return newValkeyBufferFromClient(client, opts.MaxTTL, opts.Metrics, opts.Logger), nil
}

func dialWithBackoff(ctx context.Context, opts ValkeyOptions) (valkey.Client, error) {
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = dialInitialBackoff
	eb.MaxInterval = dialMaxBackoff

	client, err := backoff.Retry(ctx, func() (valkey.Client, error) {
		return valkey.NewClient(valkey.ClientOption{
			InitAddress: []string{opts.Addr},
			SelectDB:    opts.DB,
			Password:    opts.Password,
		})
	},
		backoff.WithBackOff(eb),
		backoff.WithMaxElapsedTime(0),
		backoff.WithNotify(func(err error, next time.Duration) {
			opts.Logger.Warn("valkey dial failed, retrying",
				zap.Error(err), zap.Duration("next_attempt_in", next))
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("valkey client: %w", err)
	}
	return client, nil
}

func newValkeyBufferFromClient(client valkey.Client, maxTTL time.Duration, metrics *Metrics, logger *zap.Logger) *ValkeyBuffer {
	return &ValkeyBuffer{
		client:  client,
		maxTTL:  maxTTL,
		metrics: metrics,
		logger:  logger,
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

	if len(raw) == 0 {
		// Hash already expired or was drained by a concurrent sweeper; the
		// caller receives an empty map and skips the trace without counting
		// it as an error.
		b.logger.Debug("drain: trace already absent", zap.String("trace_id", traceHex))
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
