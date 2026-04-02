package reconnect

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"stuurwiel/internal/config"
)

func testReconnectConfig() config.Config {
	return config.Config{
		ForwardProbability:    0.8,
		WorkerConcurrency:     10,
		ReconnectInitialDelay: 2 * time.Millisecond,
		ReconnectMaxDelay:     50 * time.Millisecond,
		ReconnectJitterMax:    0,
		MaxReconnectAttempts:  20,
	}
}

func TestDialUntil_SucceedsAfterFailures(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testReconnectConfig()
	var calls int
	err := DialUntil(context.Background(), log, cfg, NewAttemptLimiter(10), "test", func() error {
		calls++
		if calls < 3 {
			return errors.New("dial failed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 dials, got %d", calls)
	}
}

func TestDialUntil_ReconnectLimit(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testReconnectConfig()
	err := DialUntil(context.Background(), log, cfg, NewAttemptLimiter(2), "test", func() error {
		return errors.New("always fail")
	})
	if err == nil || !errors.Is(err, ErrReconnectLimit) {
		t.Fatalf("expected ErrReconnectLimit, got %v", err)
	}
}

func TestDialUntil_ContextCanceled(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testReconnectConfig()
	cfg.ReconnectInitialDelay = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	err := DialUntil(ctx, log, cfg, NewAttemptLimiter(100), "test", func() error {
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetryDelayMs(t *testing.T) {
	if got := RetryDelayMs(1500 * time.Microsecond); got != 2 {
		t.Fatalf("round to nearest ms: got %d", got)
	}
	if got := RetryDelayMs(0); got != 0 {
		t.Fatalf("got %d", got)
	}
	if got := RetryDelayMs(-time.Second); got != 0 {
		t.Fatalf("got %d", got)
	}
}

func TestNextDelay(t *testing.T) {
	if d := NextDelay(2*time.Second, time.Minute); d != 4*time.Second {
		t.Fatalf("got %v", d)
	}
	if d := NextDelay(90*time.Second, time.Minute); d != time.Minute {
		t.Fatalf("got %v", d)
	}
}
