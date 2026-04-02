package worker

import (
	"context"
	"errors"
	"log/slog"

	"stuurwiel/internal/config"
	"stuurwiel/internal/runtime/reconnect"
)

// ServeReconnect runs session in a loop; on recoverable failure, backoff and retry until cancel or limit.
func ServeReconnect(ctx context.Context, log *slog.Logger, cfg config.Config, edgeName string, limiter *reconnect.AttemptLimiter, session func(context.Context) error) error {
	runDelay := cfg.ReconnectInitialDelay
	for {
		err := session(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		if errors.Is(err, reconnect.ErrReconnectLimit) {
			return err
		}
		if err := limiter.RecordFailure(); err != nil {
			return err
		}
		nextWait := reconnect.DelayWithJitter(runDelay, cfg.ReconnectJitterMax)
		log.Warn("session ended, reconnecting",
			"edge", edgeName,
			"err", err,
			"reconnect_attempt", limiter.Failures(),
			"reconnect_max_attempts", limiter.Max(),
			"retry_delay_ms", reconnect.RetryDelayMs(nextWait),
		)
		if err := reconnect.SleepFor(ctx, nextWait); err != nil {
			return err
		}
		runDelay = reconnect.NextDelay(runDelay, cfg.ReconnectMaxDelay)
	}
}
