package reconnect

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"stuurwiel/internal/config"
)

// RetryDelayMs returns d rounded to the nearest millisecond for logging (avoids JSON nanoseconds from time.Duration).
func RetryDelayMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Round(float64(d) / float64(time.Millisecond)))
}

// DialUntil retries dial with exponential backoff until success, ctx done, or limiter exceeded.
func DialUntil(ctx context.Context, log *slog.Logger, cfg config.Config, limiter *AttemptLimiter, component string, dial func() error) error {
	delay := cfg.ReconnectInitialDelay
	for {
		err := dial()
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		if err := limiter.RecordFailure(); err != nil {
			return err
		}
		nextWait := DelayWithJitter(delay, cfg.ReconnectJitterMax)
		log.Warn("dial failed",
			"component", component,
			"err", err,
			"reconnect_attempt", limiter.Failures(),
			"reconnect_max_attempts", limiter.Max(),
			"retry_delay_ms", RetryDelayMs(nextWait),
		)
		if err := SleepFor(ctx, nextWait); err != nil {
			return err
		}
		delay = NextDelay(delay, cfg.ReconnectMaxDelay)
	}
}

// NextDelay returns d*2 capped at max.
func NextDelay(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}
