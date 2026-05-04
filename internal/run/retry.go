package run

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v5"
	"go.uber.org/zap"
)

// Backoff parameterizes Retry's exponential schedule.
type Backoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	// Jitter is the randomization factor applied to each interval, in [0, 1].
	Jitter float64
}

// Retry runs op until it returns nil or ctx is cancelled. Each failure is
// logged at warn level with the next delay and the wrapped error. There is
// no built-in attempt cap; the caller's ctx deadline is the source of truth
// for "give up" so Kubernetes startup probes remain authoritative.
//
// op should treat its parameter ctx as the same ctx Retry was called with;
// returning a backoff.PermanentError stops retries early.
func Retry(ctx context.Context, name string, op func(context.Context) error, b Backoff, logger *zap.Logger) error {
	if logger == nil {
		logger = zap.NewNop()
	}
	if b.Initial <= 0 {
		return errors.New("retry: backoff.initial must be > 0")
	}
	if b.Max < b.Initial {
		b.Max = b.Initial
	}

	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = b.Initial
	eb.MaxInterval = b.Max
	if b.Multiplier > 0 {
		eb.Multiplier = b.Multiplier
	}
	eb.RandomizationFactor = b.Jitter

	notify := func(err error, d time.Duration) {
		logger.Warn("retry: attempt failed",
			zap.String("name", name),
			zap.Duration("next_attempt_in", d),
			zap.Error(err),
		)
	}

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		return struct{}{}, op(ctx)
	}, backoff.WithBackOff(eb), backoff.WithNotify(notify), backoff.WithMaxElapsedTime(0))
	return err
}
