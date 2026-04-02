package reconnect

import (
	"context"
	"math/rand/v2"
	"time"
)

// DelayWithJitter returns base + uniform random jitter in [0, jitterMax] (0 disables jitter).
func DelayWithJitter(base, jitterMax time.Duration) time.Duration {
	return base + jitterDuration(jitterMax)
}

// SleepFor waits for d or until ctx is cancelled.
func SleepFor(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
	}
	return nil
}

// SleepDelay waits for base + uniform random jitter in [0, jitterMax], then returns nil.
// If jitterMax <= 0, only base is used.
func SleepDelay(ctx context.Context, base time.Duration, jitterMax time.Duration) error {
	return SleepFor(ctx, DelayWithJitter(base, jitterMax))
}

func jitterDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	// Uniform [0, max] inclusive.
	return time.Duration(rand.Int64N(max.Nanoseconds() + 1))
}
