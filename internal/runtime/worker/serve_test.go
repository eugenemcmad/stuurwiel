package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"stuurwiel/internal/config"
	"stuurwiel/internal/runtime/reconnect"
)

func TestServeReconnect_RetriesThenStopsOnCancel(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		ForwardProbability:    0.8,
		WorkerConcurrency:     10,
		ReconnectInitialDelay: 2 * time.Millisecond,
		ReconnectMaxDelay:     20 * time.Millisecond,
		ReconnectJitterMax:    0,
		MaxReconnectAttempts:  10,
	}
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			return errors.New("session boom")
		}
		<-ctx.Done()
		return ctx.Err()
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeReconnect(ctx, log, cfg, "edge-test", reconnect.NewAttemptLimiter(10), session)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected nil or canceled, got %v", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("expected at least 2 session runs, got %d", calls.Load())
	}
}

func TestServeReconnect_ReconnectLimit(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		ForwardProbability:    0.8,
		WorkerConcurrency:     10,
		ReconnectInitialDelay: time.Millisecond,
		ReconnectMaxDelay:     5 * time.Millisecond,
		ReconnectJitterMax:    0,
		MaxReconnectAttempts:  10,
	}
	err := ServeReconnect(context.Background(), log, cfg, "edge", reconnect.NewAttemptLimiter(1), func(context.Context) error {
		return errors.New("always")
	})
	if err == nil || !errors.Is(err, reconnect.ErrReconnectLimit) {
		t.Fatalf("expected ErrReconnectLimit, got %v", err)
	}
}
